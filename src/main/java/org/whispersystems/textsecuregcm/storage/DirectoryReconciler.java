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
import org.whispersystems.textsecuregcm.configuration.DirectoryServerConfiguration;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationRequest;
import org.whispersystems.textsecuregcm.redis.LuaScript;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Hex;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

public class DirectoryReconciler implements Managed, Runnable {

  private static final Logger logger = LoggerFactory.getLogger(DirectoryReconciler.class);

  private static final MetricRegistry metricRegistry      = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Timer          readChunkTimer      = metricRegistry.timer(name(DirectoryReconciler.class, "readChunk"));
  private static final Timer          sendChunkTimer      = metricRegistry.timer(name(DirectoryReconciler.class, "sendChunk"));
  private static final Meter          sendChunkErrorMeter = metricRegistry.meter(name(DirectoryReconciler.class, "sendChunkError"));

  private static final String WORKER_SET_KEY    = "directory_reconciliation_workers";
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

  private final String              replicationUrl;
  private final ReplicatedJedisPool jedisPool;
  private final AccountsManager     accountsManager;
  private final Client              client;
  private final String              workerId;
  private final SecureRandom        random;
  private final UnlockOperation     unlockOperation;

  private ScheduledExecutorService executor;

  public DirectoryReconciler(DirectoryServerConfiguration directoryServerConfiguration,
                             ReplicatedJedisPool jedisPool,
                             AccountsManager accountsManager)
      throws IOException, CertificateException
  {
    this.replicationUrl              = directoryServerConfiguration.getReplicationUrl();
    this.jedisPool                   = jedisPool;
    this.accountsManager             = accountsManager;
    this.client                      = initializeClient(directoryServerConfiguration);
    this.random                      = new SecureRandom();
    this.workerId                    = generateWorkerId(random);
    this.unlockOperation             = new UnlockOperation(jedisPool);
  }

  private static String generateWorkerId(SecureRandom random) {
    byte[] workerIdBytes = new byte[16];
    random.nextBytes(workerIdBytes);
    return Hex.toString(workerIdBytes);
  }

  private static Client initializeClient(DirectoryServerConfiguration directoryServerConfiguration)
      throws CertificateException
  {
    SslConfigurator sslConfig = SslConfigurator.newInstance()
                                               .securityProtocol("TLSv1.2");
    sslConfig.trustStore(initializeKeyStore(directoryServerConfiguration.getReplicationCaCertificate()));
    return ClientBuilder.newBuilder()
                        .register(HttpAuthenticationFeature.basic("signal", directoryServerConfiguration.getReplicationPassword().getBytes()))
                        .sslContext(sslConfig.createSSLContext())
                        .build();
  }

  private static KeyStore initializeKeyStore(String caCertificatePem)
      throws CertificateException
  {
    try {
      PEMReader       reader      = new PEMReader(new InputStreamReader(new ByteArrayInputStream(caCertificatePem.getBytes())));
      X509Certificate certificate = (X509Certificate) reader.readObject();

      if (certificate == null) {
        throw new CertificateException("No certificate found in parsing!");
      }

      KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      keyStore.load(null);
      keyStore.setCertificateEntry("ca", certificate);
      return keyStore;
    } catch (IOException | KeyStoreException ex) {
      throw new CertificateException(ex);
    } catch (NoSuchAlgorithmException ex) {
      throw new AssertionError(ex);
    }
  }

  @Override
  public void start() {
    this.executor = Executors.newSingleThreadScheduledExecutor();

    try (Jedis jedis = jedisPool.getWriteResource()) {
      cleanUpWorkerSet(jedis);
    }

    this.executor.scheduleWithFixedDelay(new PingTask(jedisPool, workerId), 0, WORKER_TTL_MS / 2, TimeUnit.MILLISECONDS);
    this.executor.submit(this);
  }

  @Override
  public void stop() {
    this.executor.shutdown();

    try (Jedis jedis = jedisPool.getWriteResource()) {
      leaveWorkerSet(jedis, workerId);
    }

    try {
      this.executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      throw new AssertionError(ex);
    }
  }

  @Override
  public void run() {
    boolean accelerate  = false;
    long    workerCount = 1;

    try (Jedis jedis = jedisPool.getWriteResource()) {
      if (lockActiveWorker(jedis, workerId)) {
        try {
          processChunk(jedis);
          accelerate = isAccelerated(jedis);
        } finally {
          unlockActiveWorker(unlockOperation, workerId);
        }
      }

      if (!accelerate) {
        workerCount = Math.max(getWorkerCount(jedis), 1);
      }
    } catch (Exception ex) {
      logger.warn("error in directory reconciliation", ex);
    }

    if (!accelerate) {
      scheduleWithJitter(CHUNK_INTERVAL * workerCount);
    } else {
      scheduleWithJitter(ACCELERATE_CHUNK_INTERVAL);
    }
  }

  private void scheduleWithJitter(long delayMs) {
    long randomJitterMs = (long) (random.nextDouble() * JITTER_MAX * delayMs);
    executor.schedule(this, delayMs + randomJitterMs, TimeUnit.MILLISECONDS);
  }

  private void processChunk(Jedis jedis) {
    Optional<String> fromNumber = getLastNumber(jedis);
    int              chunkSize  = (int) (getAccountCount(jedis) * PERIOD / CHUNK_INTERVAL);
    List<String>     numbers    = readChunk(fromNumber, chunkSize);

    Optional<String> toNumber = Optional.absent();
    if (!numbers.isEmpty()) {
      toNumber = Optional.of(numbers.get(numbers.size() - 1));
    }

    Response sendChunkResponse   = sendChunk(fromNumber, numbers);
    boolean  sendChunkSuccessful = sendChunkResponse.getStatusInfo().getFamily() == Response.Status.Family.SUCCESSFUL;

    if (!sendChunkSuccessful || !toNumber.isPresent()) {
      clearAccelerate(jedis);
    }

    if (sendChunkSuccessful) {
      setLastNumber(jedis, toNumber);
    } else if (sendChunkResponse.getStatus() == 404) {
      setLastNumber(jedis, Optional.absent());
    }
  }

  private List<String> readChunk(Optional<String> fromNumber, int chunkSize) {
    try (Timer.Context timer = readChunkTimer.time()) {
      if (fromNumber.isPresent()) {
        return accountsManager.getAllNumbers(fromNumber.get(), chunkSize);
      } else {
        return accountsManager.getAllNumbers(chunkSize);
      }
    }
  }

  private Response sendChunk(Optional<String> fromNumber, List<String> numbers) {
    String reconcilePath;
    if (fromNumber.isPresent()) {
      reconcilePath = String.format("/v1/directory/reconcile/%s", fromNumber);
    } else {
      reconcilePath = "/v1/directory/reconcile";
    }

    try (Timer.Context timer = sendChunkTimer.time()) {
      Response response = client.target(replicationUrl)
                                .path(reconcilePath)
                                .request(MediaType.APPLICATION_JSON)
                                .put(Entity.json(new DirectoryReconciliationRequest(numbers)));
      if (response.getStatusInfo().getFamily() != Response.Status.Family.SUCCESSFUL) {
        sendChunkErrorMeter.mark();
        logger.warn("http error " + response.getStatus());
      }
      return response;
    } catch (ProcessingException ex) {
      sendChunkErrorMeter.mark();
      logger.warn("request error: ", ex);
      throw ex;
    }
  }

  private static void cleanUpWorkerSet(Jedis jedis) {
    try {
      long nowMs = System.currentTimeMillis();
      jedis.zremrangeByScore(WORKER_SET_KEY, Double.NEGATIVE_INFINITY, (double) (nowMs - WORKER_TTL_MS));
    } catch (Exception ex) {
      logger.warn("failed to clean up worker set", ex);
    }
  }

  private static void joinWorkerSet(Jedis jedis, String workerId) {
    long nowMs = System.currentTimeMillis();
    jedis.zadd(WORKER_SET_KEY, (double) nowMs, workerId);
  }

  private static void leaveWorkerSet(Jedis jedis, String workerId) {
    try {
      jedis.zrem(WORKER_SET_KEY, workerId);
    } catch (Exception ex) {
      logger.warn("failed to remove from worker set", ex);
    }
  }

  private static long getWorkerCount(Jedis jedis) {
    long nowMs = System.currentTimeMillis();
    return jedis.zcount(WORKER_SET_KEY, (double) (nowMs - WORKER_TTL_MS), Double.POSITIVE_INFINITY);
  }

  private static void clearAccelerate(Jedis jedis) {
    jedis.del(ACCELERATE_KEY);
  }

  private static boolean isAccelerated(Jedis jedis) {
    return "1".equals(jedis.get(ACCELERATE_KEY));
  }

  private static boolean lockActiveWorker(Jedis jedis, String workerId) {
    return "OK".equals(jedis.set(ACTIVE_WORKER_KEY, workerId, "NX", "PX", CHUNK_INTERVAL));
  }

  private static void unlockActiveWorker(UnlockOperation unlockOperation, String workerId) {
    unlockOperation.unlock(ACTIVE_WORKER_KEY, workerId);
  }

  private static Optional<String> getLastNumber(Jedis jedis) {
    return Optional.fromNullable(jedis.get(LAST_NUMBER_KEY));
  }

  private static void setLastNumber(Jedis jedis, Optional<String> lastNumber) {
    if (lastNumber.isPresent()) {
      jedis.set(LAST_NUMBER_KEY, lastNumber.get());
    } else {
      jedis.del(LAST_NUMBER_KEY);
    }
  }


  private long getAccountCount(Jedis jedis) {
    Optional<Long> cachedCount = getCachedAccountCount(jedis);

    if (cachedCount.isPresent()) {
      return cachedCount.get();
    }

    long count = accountsManager.getCount();
    setCachedAccountCount(jedis, count);
    return count;
  }

  private static Optional<Long> getCachedAccountCount(Jedis jedis) {
    Optional<String> cachedAccountCount = Optional.fromNullable(jedis.get(CACHED_COUNT_KEY));
    if (!cachedAccountCount.isPresent()) {
      return Optional.absent();
    }

    try {
      return Optional.of(Long.parseLong(cachedAccountCount.get()));
    } catch (NumberFormatException ex) {
      return Optional.absent();
    }
  }

  private static void setCachedAccountCount(Jedis jedis, long accountCount) {
    jedis.psetex(CACHED_COUNT_KEY, CACHED_COUNT_TTL_MS, Long.toString(accountCount));
  }

  private static class PingTask implements Runnable {

    private final String              workerId;
    private final ReplicatedJedisPool jedisPool;

    PingTask(ReplicatedJedisPool jedisPool, String workerId) {
      this.jedisPool = jedisPool;
      this.workerId  = workerId;
    }

    @Override
    public void run() {
      try (Jedis jedis = jedisPool.getWriteResource()) {
        joinWorkerSet(jedis, workerId);
      } catch (Exception ex) {
        logger.warn("error joining worker set: ", ex);
      }
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
