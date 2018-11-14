/*
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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Util;

import java.security.SecureRandom;
import java.util.List;
import java.util.LinkedList;
import java.util.Optional;
import java.util.UUID;

import static com.codahale.metrics.MetricRegistry.name;
import io.dropwizard.lifecycle.Managed;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class AccountDatabaseCrawler implements Managed, Runnable {

  private static final Logger         logger         = LoggerFactory.getLogger(AccountDatabaseCrawler.class);
  private static final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Timer          readChunkTimer = metricRegistry.timer(name(AccountDatabaseCrawler.class, "readChunk"));

  private static final long   WORKER_TTL_MS              = 120_000L;
  private static final long   MINIMUM_CHUNK_INTERVAL     = 500L;
  private static final long   ACCELERATED_CHUNK_INTERVAL = 10L;
  private static final double JITTER_MAX                 = 0.20;

  private final Accounts                             accounts;
  private final int                                  chunkSize;
  private final long                                 chunkIntervalMs;
  private final String                               workerId;
  private final AccountDatabaseCrawlerCache          cache;
  private final List<AccountDatabaseCrawlerListener> listeners;

  private boolean running;
  private boolean finished;

  public AccountDatabaseCrawler(Accounts accounts,
                                AccountDatabaseCrawlerCache cache,
                                List<AccountDatabaseCrawlerListener> listeners,
                                int chunkSize,
                                long chunkIntervalMs) {
    this.accounts             = accounts;
    this.chunkSize            = chunkSize;
    this.chunkIntervalMs      = chunkIntervalMs;
    this.workerId             = UUID.randomUUID().toString();
    this.cache                = cache;
    this.listeners            = listeners;
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
    long delayMs = chunkIntervalMs;

    while (sleepWhileRunning(getDelayWithJitter(delayMs))) {
      try {
        delayMs = getBoundedChunkInterval(chunkIntervalMs);
        delayMs = doPeriodicWork(delayMs);
      } catch (Throwable t) {
        logger.warn("error in database crawl: ", t);
      }
    }

    synchronized (this) {
      finished = true;
      notifyAll();
    }
  }

  @VisibleForTesting
  public long doPeriodicWork(long intervalMs) {
    long nextIntervalTimeMs = System.currentTimeMillis() + intervalMs;
    if (cache.claimActiveWork(workerId, WORKER_TTL_MS)) {
      processChunk();
      if (!cache.isAccelerated()) {
        long timeUntilNextIntervalMs = getTimeUntilNextInterval(nextIntervalTimeMs);
        cache.claimActiveWork(workerId, timeUntilNextIntervalMs);
        return timeUntilNextIntervalMs;
      } else {
        return ACCELERATED_CHUNK_INTERVAL;
      }
    }
    return intervalMs;
  }

  private void processChunk() {
    Optional<String> fromNumber = cache.getLastNumber();

    if (!fromNumber.isPresent()) {
      for (AccountDatabaseCrawlerListener listener : listeners) listener.onCrawlStart();
    }

    List<Account> chunkAccounts = readChunk(fromNumber, chunkSize);

    if (chunkAccounts.isEmpty()) {
      for (AccountDatabaseCrawlerListener listener : listeners)
        listener.onCrawlEnd();
      cache.setLastNumber(Optional.empty());
    } else {
      for (AccountDatabaseCrawlerListener listener : listeners)
        listener.onCrawlChunk(chunkAccounts);
      cache.setLastNumber(Optional.of(chunkAccounts.get(chunkAccounts.size() - 1).getNumber()));
    }
    
  }

  private List<Account> readChunk(Optional<String> fromNumber, int chunkSize) {
    try (Timer.Context timer = readChunkTimer.time()) {
      List<Account> chunkAccounts;

      if (fromNumber.isPresent()) {
        chunkAccounts = accounts.getAllFrom(fromNumber.get(), chunkSize);
      } else {
        chunkAccounts = accounts.getAllFrom(chunkSize);
      }

      return chunkAccounts;
    }
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
    long nextIntervalMs = nextIntervalTimeMs - System.currentTimeMillis();
    return getBoundedChunkInterval(nextIntervalMs);
  }

  private long getBoundedChunkInterval(long intervalMs) {
    return Math.max(Math.min(intervalMs, chunkIntervalMs), MINIMUM_CHUNK_INTERVAL);
  }

  private long getDelayWithJitter(long delayMs) {
    long randomJitterMs = (long) (new SecureRandom().nextDouble() * JITTER_MAX * delayMs);
    return delayMs + randomJitterMs;
  }

}
