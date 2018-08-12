package org.whispersystems.textsecuregcm.tests.util;

import com.google.common.util.concurrent.SettableFuture;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SynchronousExecutorService implements ScheduledExecutorService {

  private boolean shutdown = false;

  private PriorityQueue<ScheduledTask> scheduled = new PriorityQueue<>();

  @Override
  public void shutdown() {
    shutdown = true;
  }

  @Override
  public List<Runnable> shutdownNow() {
    shutdown = true;
    return Collections.emptyList();
  }

  @Override
  public boolean isShutdown() {
    return shutdown;
  }

  @Override
  public boolean isTerminated() {
    return shutdown;
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    while (!scheduled.isEmpty()) {
      try {
        scheduled.poll().get();
      } catch (ExecutionException ex) {
        throw new AssertionError(ex.getCause());
      }
    }
    return shutdown;
  }

  private void checkShutdown() {
    if (shutdown) {
      throw new RejectedExecutionException();
    }
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    checkShutdown();
    SettableFuture<T> future = null;
    try {
      future = SettableFuture.create();
      future.set(task.call());
    } catch (Throwable e) {
      future.setException(e);
    }

    return future;
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    checkShutdown();
    SettableFuture<T> future = SettableFuture.create();
    task.run();

    future.set(result);

    return future;
  }

  @Override
  public Future<?> submit(Runnable task) {
    checkShutdown();
    SettableFuture future = SettableFuture.create();
    task.run();
    future.set(null);
    return future;
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
    checkShutdown();
    List<Future<T>> results = new LinkedList<>();
    for (Callable<T> callable : tasks) {
      SettableFuture<T> future = SettableFuture.create();
      try {
        future.set(callable.call());
      } catch (Throwable e) {
        future.setException(e);
      }
      results.add(future);
    }
    return results;
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
    return invokeAll(tasks);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
    return null;
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    return null;
  }

  @Override
  public void execute(Runnable command) {
    checkShutdown();
    command.run();
  }

  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
    checkShutdown();
    ScheduledTask task = new ScheduledTask(callable, delay, unit);
    scheduled.add(task);
    return task;
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
    return schedule(() -> { command.run(); return null; }, delay, unit);
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long delay, TimeUnit unit) {
    return schedule(command, initialDelay, unit);
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
    return schedule(command, initialDelay, unit);
  }

  private static class ScheduledTask<V> implements ScheduledFuture<V> {

    private final Callable<V>       command;
    private final SettableFuture<V> future;
    private final long              delay;
    private final TimeUnit          unit;

    ScheduledTask(Callable<V> command, long delay, TimeUnit unit) {
      this.command = command;
      this.future  = SettableFuture.create();
      this.delay   = delay;
      this.unit    = unit;
    }

    @Override
    public long getDelay(TimeUnit newUnit) {
      return newUnit.convert(delay, unit);
    }

    @Override
    public int compareTo(Delayed delayed) {
      return Long.compare(getDelay(TimeUnit.MILLISECONDS), delayed.getDelay(TimeUnit.MILLISECONDS));
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
      return future.isCancelled();
    }

    @Override
    public boolean isDone() {
      return future.isDone();
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
      if (!future.isDone()) {
        try {
          future.set(command.call());
        } catch (Throwable t) {
          future.setException(t);
        }
      }
      return future.get();
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      return get();
    }
  }

}
