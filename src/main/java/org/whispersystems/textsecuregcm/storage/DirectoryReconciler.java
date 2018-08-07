/**
 * Copyright (C) 2018 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.textsecuregcm.storage;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.common.base.Optional;
import io.dropwizard.lifecycle.Managed;
import org.bouncycastle.openssl.PEMReader;
import org.glassfish.jersey.SslConfigurator;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.ContactDiscoveryConfiguration;
import org.whispersystems.textsecuregcm.configuration.DirectoryConfiguration;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationRequest;
import org.whispersystems.textsecuregcm.redis.LuaScript;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Hex;
import org.whispersystems.textsecuregcm.util.Util;
import redis.clients.jedis.Jedis;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static com.codahale.metrics.MetricRegistry.name;

public class DirectoryReconciler implements Managed, Runnable {

  private static final Logger logger = LoggerFactory.getLogger(DirectoryReconciler.class);

  private static final MetricRegistry metricRegistry      = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Timer          readChunkTimer      = metricRegistry.timer(name(DirectoryReconciler.class, "readChunk"));
  private static final Timer          sendChunkTimer      = metricRegistry.timer(name(DirectoryReconciler.class, "sendChunk"));
  private static final Meter          sendChunkErrorMeter = metricRegistry.meter(name(DirectoryReconciler.class, "sendChunkError"));

  private static final String WORKERS_KEY       = "directory_reconciliation_workers";
  private static final String ACTIVE_WORKER_KEY = "directory_reconciliation_active_worker";
  private static final String LAST_NUMBER_KEY   = "directory_reconciliation_last_number";
  private static final String CACHED_COUNT_KEY  = "directory_reconciliation_cached_count";
  private static final String ACCELERATE_KEY    = "directory_reconciliation_accelerate";

  private static final long   CACHED_COUNT_TTL_MS       = 21600_000L;
  private static final long   WORKER_TTL_MS             = 120_000L;
  private static final long   PERIOD                    = 86400_000L;
  private static final long   CHUNK_INTERVAL            = 42_000L;
  private static final long   ACCELERATE_CHUNK_INTERVAL = 500L;
  private static final double JITTER_MAX                = 0.20;

  private final String              serverApiUrl;
  private final ReplicatedJedisPool jedisPool;
  private final AccountsManager     accountsManager;
  private final Client              client;

  private final String       workerId;
  private final SecureRandom random;

  private final UnlockOperation unlockOperation;

  private boolean running;
  private boolean finished;


  public DirectoryReconciler(ContactDiscoveryConfiguration cdsConfig,
                             ReplicatedJedisPool jedisPool,
                             AccountsManager accountsManager)
      throws IOException
  {
    DirectoryConfiguration directoryConfig = cdsConfig.getDirectoryConfiguration();
    this.serverApiUrl    = directoryConfig.getServerApiUrl();
    this.jedisPool       = jedisPool;
    this.accountsManager = accountsManager;

    this.random = new SecureRandom();

    byte[] workerIdBytes = new byte[16];
    random.nextBytes(workerIdBytes);
    this.workerId = Hex.toString(workerIdBytes);

    this.unlockOperation = new UnlockOperation(jedisPool);

    SslConfigurator sslConfig = SslConfigurator.newInstance()
                                               .securityProtocol("TLSv1.2");
    try {
      sslConfig.trustStore(initializeKeyStore(directoryConfig.getServerApiCaCertificate()));
    } catch (CertificateException ex) {
      logger.error("error reading serverApiCaCertificate from contactDiscovery config", ex);
    }

    this.client = ClientBuilder.newBuilder()
                               .register(HttpAuthenticationFeature.basic("signal", directoryConfig.getServerApiToken().getBytes()))
                               .sslContext(sslConfig.createSSLContext())
                               .build();
  }

  private static KeyStore initializeKeyStore(String pemCaCertificate)
          throws CertificateException
  {
    try {
      PEMReader       reader      = new PEMReader(new InputStreamReader(new ByteArrayInputStream(pemCaCertificate.getBytes())));
      X509Certificate certificate = (X509Certificate) reader.readObject();

      if (certificate == null) {
        throw new CertificateException("No certificate found in parsing!");
      }

      KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      keyStore.load(null);
      keyStore.setCertificateEntry("ca", certificate);
      return keyStore;
    } catch (IOException | KeyStoreException ex) {
      throw new CertificateException(ex.toString());
    } catch (NoSuchAlgorithmException ex) {
      throw new AssertionError(ex);
    }
  }

  @Override
  public synchronized void start() throws Exception {
    running  = true;
    finished = false;
    new Thread(this).start();
  }

  @Override
  public synchronized void stop() throws Exception {
    running = false;
    while (!finished) Util.wait(this);
  }

  private synchronized boolean sleepWhileRunning(long delay) {
    long start   = System.currentTimeMillis();
    long elapsed = 0;
    while (running && elapsed < delay) {
      try {
        wait(delay - elapsed);
      } catch (InterruptedException ex) {
      }
      elapsed = System.currentTimeMillis() - start;
    }
    return running;
  }

  private List<String> readChunk(Optional<String> from, int chunkSize) {
    try (Timer.Context timer = readChunkTimer.time()) {
      if (from.isPresent()) {
        return accountsManager.getAllNumbers(from.get(), chunkSize);
      } else {
        return accountsManager.getAllNumbers(chunkSize);
      }
    }
  }

  private Optional<Response> sendChunk(Optional<String> fromNumber, List<String> numbers) {
    String reconcilePath;
    if (fromNumber.isPresent()) {
      reconcilePath = String.format("/v1/directory/reconcile/%s", fromNumber);
    } else {
      reconcilePath = "/v1/directory/reconcile";
    }

    try (Timer.Context timer = sendChunkTimer.time()) {
      Response response = client.target(serverApiUrl)
                                .path(reconcilePath)
                                .request(MediaType.APPLICATION_JSON)
                                .put(Entity.json(new DirectoryReconciliationRequest(numbers)));
      if (response.getStatus() != 200) {
        sendChunkErrorMeter.mark();
        logger.warn("http error " + response.getStatus());
      }
      return Optional.of(response);
    } catch (ProcessingException ex) {
      sendChunkErrorMeter.mark();
      logger.warn("request error: ", ex);
      return Optional.absent();
    }
  }

  private void processChunk(Jedis jedis) {
    Optional<String> fromNumber     = Optional.fromNullable(jedis.get(LAST_NUMBER_KEY));
    Optional<String> cachedCountStr = Optional.fromNullable(jedis.get(CACHED_COUNT_KEY));
    Optional<Long>   cachedCount    = Optional.absent();
    if (cachedCountStr.isPresent()) {
      cachedCount = tryParseLong(cachedCountStr.get());
    }

    if (!cachedCount.isPresent()) {
      cachedCount = Optional.of(accountsManager.getCount());
      jedis.psetex(CACHED_COUNT_KEY, CACHED_COUNT_TTL_MS, Long.toString(cachedCount.get()));
    }

    int          chunkSize = (int) (cachedCount.get() * PERIOD / CHUNK_INTERVAL);
    List<String> numbers   = readChunk(fromNumber, chunkSize);

    Optional<String> toNumber = Optional.absent();
    if (!numbers.isEmpty()) {
      toNumber = Optional.of(numbers.get(numbers.size() - 1));
    }

    Optional<Response> sendChunkResp = sendChunk(fromNumber, numbers);
    if (sendChunkResp.isPresent()) {
      if (sendChunkResp.get().getStatus() == 200) {
        if (toNumber.isPresent()) {
          jedis.set(LAST_NUMBER_KEY, toNumber.get());
        } else {
          jedis.del(ACCELERATE_KEY);
          jedis.del(LAST_NUMBER_KEY);
        }
      } else {
        jedis.del(ACCELERATE_KEY);
        if (sendChunkResp.get().getStatus() == 404) {
          jedis.del(LAST_NUMBER_KEY);
        }
      }
    }
  }

  @Override
  public void run() {
    logger.info("Directory reconciliation worker " + workerId + " started");

    try (Jedis jedis = jedisPool.getWriteResource()) {
      long nowMs = System.currentTimeMillis();
      jedis.zremrangeByScore(WORKERS_KEY, Double.NEGATIVE_INFINITY, (double) (nowMs - WORKER_TTL_MS));
    } catch (Exception ex) {
      logger.warn("failed to clean up worker set", ex);
    }

    long workIntervalMs = CHUNK_INTERVAL;
    long lastWorkTimeMs = 0;
    long lastPingTimeMs = 0;
    long randomJitterMs = 0;
    while (sleepWhileRunning(Math.min(lastWorkTimeMs + workIntervalMs + randomJitterMs,
                                      lastPingTimeMs + WORKER_TTL_MS / 2) - System.currentTimeMillis())) {
      long nowMs = System.currentTimeMillis();

      lastPingTimeMs = nowMs;

      try (Jedis jedis = jedisPool.getWriteResource()) {
        jedis.zadd(WORKERS_KEY, (double) nowMs, workerId);

        if (nowMs - lastWorkTimeMs > workIntervalMs + randomJitterMs) {
          lastWorkTimeMs = nowMs;

          boolean accelerate = false;
          if ("OK".equals(jedis.set(ACTIVE_WORKER_KEY, workerId, "NX", "PX", CHUNK_INTERVAL))) {
            try {
              processChunk(jedis);
              accelerate = "1".equals(jedis.get(ACCELERATE_KEY));
            } finally {
              unlockOperation.unlock(ACTIVE_WORKER_KEY, workerId);
            }
          }

          if (!accelerate) {
            long workerCount = jedis.zcount(WORKERS_KEY, (double) (nowMs - WORKER_TTL_MS), Double.POSITIVE_INFINITY);

            workIntervalMs = CHUNK_INTERVAL * workerCount;
            randomJitterMs = (long) (random.nextDouble() * JITTER_MAX * workIntervalMs);
          } else {
            workIntervalMs = ACCELERATE_CHUNK_INTERVAL;
            randomJitterMs = 0;
          }
        } else if (lastWorkTimeMs > nowMs) {
          lastWorkTimeMs = nowMs;
        }
      } catch (Exception ex) {
        logger.warn("error in directory reconciliation", ex);
        lastWorkTimeMs = nowMs;
      }
    }

    try (Jedis jedis = jedisPool.getWriteResource()) {
      jedis.zrem(WORKERS_KEY, workerId);
    } catch (Exception ex) {
      logger.warn("failed to remove from worker set", ex);
    }

    logger.info("Directory reconciliation worker " + workerId + " shut down");

    synchronized (this) {
      finished = true;
      notifyAll();
    }
  }

  public static Optional<Long> tryParseLong(String str) {
    try {
      return Optional.of(Long.getLong(str));
    } catch (NumberFormatException ex) {
      return Optional.absent();
    }
  }

  public static class UnlockOperation {

    private final LuaScript luaScript;

    UnlockOperation(ReplicatedJedisPool jedisPool) throws IOException {
      this.luaScript = LuaScript.fromResource(jedisPool, "lua/unlock.lua");
    }

    boolean unlock(String key, String value) {
      List<byte[]> keys = Arrays.asList(key.getBytes());
      List<byte[]> args = Arrays.asList(value.getBytes());

      return ((long)luaScript.execute(keys, args)) > 0;
    }
  }

}
