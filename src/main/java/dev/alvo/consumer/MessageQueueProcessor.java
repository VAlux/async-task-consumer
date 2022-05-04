package dev.alvo.consumer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;

public final class MessageQueueProcessor<T> {

  // All temporal constants are in SECONDS
  private static final int POLL_TIMEOUT = 10;
  private static final long WORKER_KEEP_ALIVE_TIME = 60;
  private static final int CORE_POOL_SIZE = 0;

  private final ExecutorService executor;
  private final AtomicBoolean isRunning;
  private final AtomicInteger tasksRunning;
  private final TopicConsumerAssociation<T> association;
  private final int pollTimeout;

  public MessageQueueProcessor(TopicConsumerAssociation<T> association) {
    this(association, association.getConsumers().size(), POLL_TIMEOUT, WORKER_KEEP_ALIVE_TIME);
  }

  public MessageQueueProcessor(int workersAmount, TopicConsumerAssociation<T> association) {
    this(association, workersAmount, POLL_TIMEOUT, WORKER_KEEP_ALIVE_TIME);
  }

  public MessageQueueProcessor(TopicConsumerAssociation<T> association,
                               int workersCapacity,
                               int pollTimeout,
                               long workerKeepAliveTime) {
    this.association = association;
    this.pollTimeout = pollTimeout;
    this.isRunning = new AtomicBoolean(false);
    this.tasksRunning = new AtomicInteger(0);

    this.executor = new ThreadPoolExecutor(
        CORE_POOL_SIZE,
        workersCapacity,
        workerKeepAliveTime, SECONDS,
        new SynchronousQueue<>());
  }

  public void launchProcessing() {
    if (isRunning.compareAndSet(false, true)) {
      for (MessageConsumer<T> consumer : association.getConsumers()) {
        executor.execute(() -> launchProcessing(consumer));
      }
    }
  }

  private void launchProcessing(final MessageConsumer<T> consumer) {
    System.out.println("New Processing session starting for topic" + association.getTopic().getName());
    System.out.println(tasksRunning.incrementAndGet() + " consumption tasks are running...");

    while (isRunning()) {
      try {
        association.getTopic()
            .pollMessage(pollTimeout, SECONDS)
            .map(consumer::consume)
            .orElseGet(this::finishProcessing);
      } catch (Exception ex) {
        System.err.println("Unexpected error during task processing: " + ex.getMessage());
        ex.printStackTrace();
      }
    }
  }

  private boolean finishProcessing() {
    if (tasksRunning.decrementAndGet() == 0) {
      System.out.println("Queue is empty for " + pollTimeout + " seconds. Stopping consumption");
      System.out.println("Current processing session is finished");
      return isRunning.compareAndSet(true, false);
    }

    return false;
  }

  public boolean isRunning() {
    return isRunning.get();
  }
}
