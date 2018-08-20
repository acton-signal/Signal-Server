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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationRequest;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationResponse;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Hex;
import org.whispersystems.textsecuregcm.util.Util;

import javax.ws.rs.ProcessingException;
import java.security.SecureRandom;
import java.util.List;
import java.util.stream.Collectors;

import static com.codahale.metrics.MetricRegistry.name;

public class DirectoryReconciler implements Managed, Runnable {

  private static final Logger logger = LoggerFactory.getLogger(DirectoryReconciler.class);

  private static final MetricRegistry metricRegistry      = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Timer          readChunkTimer      = metricRegistry.timer(name(DirectoryReconciler.class, "readChunk"));
  private static final Timer          sendChunkTimer      = metricRegistry.timer(name(DirectoryReconciler.class, "sendChunk"));
  private static final Meter          sendChunkErrorMeter = metricRegistry.meter(name(DirectoryReconciler.class, "sendChunkError"));

  private static final long   WORKER_TTL_MS          = 120_000L;
  private static final long   PERIOD                 = 86400_000L;
  private static final long   MAXIMUM_CHUNK_INTERVAL = 120_000L;
  private static final long   DEFAULT_CHUNK_INTERVAL = 60_000L;
  private static final long   MINIMUM_CHUNK_INTERVAL = 500L;
  private static final int    CHUNK_SIZE             = 10000;
  private static final double JITTER_MAX             = 0.20;

  private final AccountsManager               accountsManager;
  private final DirectoryReconciliationClient reconciliationClient;
  private final DirectoryReconciliationCache  reconciliationCache;
  private final String                        workerId;
  private final SecureRandom                  random;

  private boolean running;
  private boolean finished;

  public DirectoryReconciler(DirectoryReconciliationClient reconciliationClient,
                             DirectoryReconciliationCache reconciliationCache,
                             AccountsManager accountsManager) {
    this.accountsManager      = accountsManager;
    this.reconciliationClient = reconciliationClient;
    this.reconciliationCache  = reconciliationCache;
    this.random               = new SecureRandom();
    this.workerId             = generateWorkerId(random);
  }

  private static String generateWorkerId(SecureRandom random) {
    byte[] workerIdBytes = new byte[16];
    random.nextBytes(workerIdBytes);
    return Hex.toString(workerIdBytes);
  }

  @Override
  public synchronized void start() {
    running = true;
    new Thread(this).start();
  }

  @Override
  public synchronized void stop() {
    running = false;
    notifyAll();
    while (!finished) {
      Util.wait(this);
    }
  }

  @Override
  public void run() {
    long delayMs = DEFAULT_CHUNK_INTERVAL;

    while (sleepWhileRunning(getDelayWithJitter(delayMs))) {
      delayMs = doPeriodicWork();
    }

    synchronized (this) {
      finished = true;
      notifyAll();
    }
  }

  @VisibleForTesting
  public long doPeriodicWork() {
    long delayMs = DEFAULT_CHUNK_INTERVAL;

    try {
      long intervalMs         = getBoundedChunkInterval(PERIOD * getAccountCount() / CHUNK_SIZE);
      long nextIntervalTimeMs = System.currentTimeMillis() + intervalMs;

      delayMs = intervalMs;

      if (reconciliationCache.claimActiveWork(workerId, WORKER_TTL_MS)) {
        if (processChunk()) {
          if (!reconciliationCache.isAccelerated()) {
            delayMs = getTimeUntilNextInterval(nextIntervalTimeMs);
            reconciliationCache.claimActiveWork(workerId, delayMs);
          } else {
            delayMs = MINIMUM_CHUNK_INTERVAL;
          }
        }
      }
    } catch (Throwable t) {
      logger.warn("error in directory reconciliation: ", t);
    }

    return delayMs;
  }

  private synchronized boolean sleepWhileRunning(long delayMs) {
    long startTimeMs = System.currentTimeMillis();
    while (running && delayMs > 0) {
      Util.wait(this, delayMs);

      long nowMs = System.currentTimeMillis();
      delayMs -= Math.abs(nowMs - startTimeMs);
    }
    return running;
  }

  private long getTimeUntilNextInterval(long nextIntervalTimeMs) {
    long nextIntervalMs = System.currentTimeMillis() - nextIntervalTimeMs;
    return getBoundedChunkInterval(nextIntervalMs);
  }

  private long getBoundedChunkInterval(long intervalMs) {
    return Math.max(Math.min(intervalMs, MAXIMUM_CHUNK_INTERVAL), MINIMUM_CHUNK_INTERVAL);
  }

  private long getDelayWithJitter(long delayMs) {
    long randomJitterMs = (long) (random.nextDouble() * JITTER_MAX * delayMs);
    return delayMs + randomJitterMs;
  }

  private boolean processChunk() {
    Optional<String>                fromNumber          = reconciliationCache.getLastNumber();
    DirectoryReconciliationRequest  request             = readChunk(fromNumber, CHUNK_SIZE);
    DirectoryReconciliationResponse sendChunkResponse   = sendChunk(request);

    if (sendChunkResponse.getStatus() == DirectoryReconciliationResponse.Status.MISSING ||
        request.getToNumber() == null) {
      reconciliationCache.clearAccelerate();
    }

    if (sendChunkResponse.getStatus() == DirectoryReconciliationResponse.Status.OK) {
      reconciliationCache.setLastNumber(Optional.fromNullable(request.getToNumber()));
    } else if (sendChunkResponse.getStatus() == DirectoryReconciliationResponse.Status.MISSING) {
      reconciliationCache.setLastNumber(Optional.absent());
    }

    return sendChunkResponse.getStatus() == DirectoryReconciliationResponse.Status.OK;
  }

  private DirectoryReconciliationRequest readChunk(Optional<String> fromNumber, int chunkSize) {
    try (Timer.Context timer = readChunkTimer.time()) {
      List<Account> accounts = accountsManager.getAllFrom(fromNumber, chunkSize);

      List<String> numbers = accounts.stream()
                                     .filter(Account::isActive)
                                     .map(Account::getNumber)
                                     .collect(Collectors.toList());

      Optional<String> toNumber = Optional.absent();
      if (!accounts.isEmpty()) {
        toNumber = Optional.of(accounts.get(accounts.size() - 1).getNumber());
      }

      return new DirectoryReconciliationRequest(fromNumber.orNull(), toNumber.orNull(), numbers);
    }
  }

  private DirectoryReconciliationResponse sendChunk(DirectoryReconciliationRequest request) {
    try (Timer.Context timer = sendChunkTimer.time()) {
      DirectoryReconciliationResponse response = reconciliationClient.sendChunk(request);
      if (response.getStatus() != DirectoryReconciliationResponse.Status.OK) {
        sendChunkErrorMeter.mark();
        logger.warn("reconciliation error: " + response.getStatus());
      }
      return response;
    } catch (ProcessingException ex) {
      sendChunkErrorMeter.mark();
      logger.warn("request error: ", ex);
      throw new ProcessingException(ex);
    }
  }



  private long getAccountCount() {
    Optional<Long> cachedCount = reconciliationCache.getCachedAccountCount();

    if (cachedCount.isPresent()) {
      return cachedCount.get();
    }

    long count = accountsManager.getCount();
    reconciliationCache.setCachedAccountCount(count);
    return count;
  }

}
