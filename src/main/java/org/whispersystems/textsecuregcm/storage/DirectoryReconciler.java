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
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Hex;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.core.Response;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

public class DirectoryReconciler implements Managed, Runnable {

  private static final Logger logger = LoggerFactory.getLogger(DirectoryReconciler.class);

  private static final MetricRegistry metricRegistry      = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Timer          readChunkTimer      = metricRegistry.timer(name(DirectoryReconciler.class, "readChunk"));
  private static final Timer          sendChunkTimer      = metricRegistry.timer(name(DirectoryReconciler.class, "sendChunk"));
  private static final Meter          sendChunkErrorMeter = metricRegistry.meter(name(DirectoryReconciler.class, "sendChunkError"));

  private static final long   WORKER_TTL_MS             = 120_000L;
  private static final long   PERIOD                    = 86400_000L;
  private static final long   CHUNK_INTERVAL            = 42_000L;
  private static final long   ACCELERATE_CHUNK_INTERVAL = 500L;
  private static final long   MINIMUM_CHUNK_SIZE        = 2000L;
  private static final double JITTER_MAX                = 0.20;

  private final AccountsManager               accountsManager;
  private final DirectoryReconciliationClient reconciliationClient;
  private final DirectoryReconciliationCache  reconciliationCache;
  private final String                        workerId;
  private final SecureRandom                  random;

  private ScheduledExecutorService executor;

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
  public void start() {
    start(Executors.newSingleThreadScheduledExecutor());
  }

  @VisibleForTesting
  public void start(ScheduledExecutorService executor) {
    this.executor = executor;

    scheduleWithJitter(CHUNK_INTERVAL);
  }

  @Override
  public void stop() {
    this.executor.shutdown();

    try {
      this.executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      throw new AssertionError(ex);
    }
  }

  @Override
  public void run() {
    boolean accelerate = false;

    try {
      if (reconciliationCache.claimActiveWork(workerId, CHUNK_INTERVAL)) {
        if (processChunk()) {
          accelerate = reconciliationCache.isAccelerated();
        }
      }
    } catch (Exception ex) {
      logger.warn("error in directory reconciliation", ex);
    }

    if (!accelerate) {
      scheduleWithJitter(CHUNK_INTERVAL);
    } else {
      scheduleWithJitter(ACCELERATE_CHUNK_INTERVAL);
    }
  }

  private void scheduleWithJitter(long delayMs) {
    long randomJitterMs = (long) (random.nextDouble() * JITTER_MAX * delayMs);
    try {
      executor.schedule(this, delayMs + randomJitterMs, TimeUnit.MILLISECONDS);
    } catch (RejectedExecutionException ex) {
      logger.info("reconciler shutting down");
    }
  }

  private boolean processChunk() {
    Optional<String>               fromNumber          = reconciliationCache.getLastNumber();
    int                            chunkSize           = (int) Math.max(getAccountCount() * PERIOD / CHUNK_INTERVAL, MINIMUM_CHUNK_SIZE);
    DirectoryReconciliationRequest request             = readChunk(fromNumber, chunkSize);
    Response                       sendChunkResponse   = sendChunk(request);
    boolean                        sendChunkSuccessful = sendChunkResponse.getStatusInfo().getFamily() == Response.Status.Family.SUCCESSFUL;

    if (sendChunkResponse.getStatus() == 404 || request.getToNumber() == null) {
      reconciliationCache.clearAccelerate();
    }

    if (sendChunkSuccessful) {
      reconciliationCache.setLastNumber(Optional.fromNullable(request.getToNumber()));
    } else if (sendChunkResponse.getStatus() == 404) {
      reconciliationCache.setLastNumber(Optional.absent());
    }

    return sendChunkSuccessful;
  }

  private DirectoryReconciliationRequest readChunk(Optional<String> fromNumber, int chunkSize) {
    try (Timer.Context timer = readChunkTimer.time()) {
      List<Account> accounts = accountsManager.getAllFrom(fromNumber, chunkSize);

      List<String> numbers = new ArrayList<>(accounts.size());
      for (Account account : accounts) {
        if (account.isActive()) {
          numbers.add(account.getNumber());
        }
      }

      Optional<String> toNumber = Optional.absent();
      if (!accounts.isEmpty()) {
        toNumber = Optional.of(accounts.get(accounts.size() - 1).getNumber());
      }

      return new DirectoryReconciliationRequest(fromNumber.orNull(), toNumber.orNull(), numbers);
    }
  }

  private Response sendChunk(DirectoryReconciliationRequest request) {
    try (Timer.Context timer = sendChunkTimer.time()) {
      Response response = reconciliationClient.sendChunk(request);
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
