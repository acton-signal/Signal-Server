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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.metrics.MetricsFactory;
import io.dropwizard.metrics.ReporterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Util;
import redis.clients.jedis.Jedis;

import java.security.SecureRandom;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class ActiveUserCounter implements AccountDatabaseCrawlerListener {

  private static final Logger logger = LoggerFactory.getLogger(ActiveUserCounter.class);

  private static final String PREFIX           = "active_user_";
  
  private static final String PLATFORM_IOS     = "ios";
  private static final String PLATFORM_ANDROID = "android";

  private static final String PLATFORMS[] = {PLATFORM_IOS, PLATFORM_ANDROID};
  private static final String INTERVALS[] = {"daily", "weekly", "monthly", "quarterly", "yearly"};

  private final MetricsFactory      metricsFactory;
  private final ReplicatedJedisPool jedisPool;

  public ActiveUserCounter(MetricsFactory metricsFactory, ReplicatedJedisPool jedisPool) {
    this.metricsFactory  = metricsFactory;
    this.jedisPool       = jedisPool;
  }

  public void onCrawlStart() {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      for (String platform : PLATFORMS) {
        for (String interval : INTERVALS) {
          jedis.set(tallyKey(platform, interval), "0");
        }
      }
    }
  }

  public void onCrawlChunk(List<Account> chunkAccounts) {
    long nowDays  = TimeUnit.MILLISECONDS.toDays(System.currentTimeMillis());
    long agoMs[]  = {TimeUnit.DAYS.toMillis(nowDays - 1),
                     TimeUnit.DAYS.toMillis(nowDays - 7),
                     TimeUnit.DAYS.toMillis(nowDays - 30),
                     TimeUnit.DAYS.toMillis(nowDays - 90),
                     TimeUnit.DAYS.toMillis(nowDays - 365)};
    long ios[]     = {0, 0, 0, 0, 0};
    long android[] = {0, 0, 0, 0, 0};

    for (Account account : chunkAccounts) {

      Optional<Device> device = account.getMasterDevice();

      if (!device.isPresent()) continue;

      long lastActiveMs = device.get().getLastSeen();

      if (device.get().getApnId() != null) {
        for (int i = 0; i < agoMs.length; i++)
          if (lastActiveMs > agoMs[i]) ios[i]++;
      } else if (device.get().getGcmId() != null) {
        for (int i = 0; i < agoMs.length; i++)
          if (lastActiveMs > agoMs[i]) android[i]++;
      }
    }
    incrementTallies(PLATFORM_IOS, INTERVALS, ios);
    incrementTallies(PLATFORM_ANDROID, INTERVALS, android);
  }

  public void onCrawlEnd() {
    MetricRegistry metrics = new MetricRegistry();
    long intervalTallies[] = new long[INTERVALS.length];
    for (String platform : PLATFORMS) {
      long platformTallies[] = getFinalTallies(platform);
      for (int i = 0; i < INTERVALS.length; i++) {
        final long tally = platformTallies[i];
        logger.info(metricKey(platform, INTERVALS[i]) + " " + tally);
        metrics.register(metricKey(platform, INTERVALS[i]),
                                new Gauge<Long>() {
                                  @Override
                                  public Long getValue() { return tally; }
                                });
        intervalTallies[i] += tally;
      }
    }

    for (int i = 0; i < INTERVALS.length; i++) {
      final long intervalTotal = intervalTallies[i];
      logger.info(metricKey(INTERVALS[i]) + " " + intervalTotal);
      metrics.register(metricKey(INTERVALS[i]),
                              new Gauge<Long>() {
                                @Override
                                public Long getValue() { return intervalTotal; }
                              });
    }
    for (ReporterFactory reporterFactory : metricsFactory.getReporters()) {
      reporterFactory.build(metrics).report();
    }
  }

  private void incrementTallies(String platform, String intervals[], long tallies[]) {
    try (Jedis jedis = jedisPool.getWriteResource()) {
      for (int i = 0; i < intervals.length; i++) {
        if (tallies[i] > 0) {
          jedis.incrBy(tallyKey(platform, intervals[i]), tallies[i]);
        }
      }
    }
  }

  private long[] getFinalTallies(String platform) {
    try (Jedis jedis = jedisPool.getReadResource()) {
      long tallies[] = new long[INTERVALS.length];
      for (int i = 0; i < INTERVALS.length; i++) {
        tallies[i] = Long.valueOf(jedis.get(tallyKey(platform, INTERVALS[i])));
      }
      return tallies;
    }
  }

  private String tallyKey(String platform, String intervalName) {
    return PREFIX + platform + "_tally_" + intervalName;
  }

  private String metricKey(String platform, String intervalName) {
    return MetricRegistry.name(ActiveUserCounter.class, intervalName + "_active_" + platform);
  }

  private String metricKey(String intervalName) {
    return MetricRegistry.name(ActiveUserCounter.class, intervalName + "_active");
  }

}
