package dev.alvo;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public final class Topic<T> {

  private final Queue<Message<T>> messageQueue;
  private final String name;
  private final ReentrantLock lock;
  private final Condition notEmpty;
  private final Condition notFull;

  public Topic(String name) {
    this.name = name;
    this.messageQueue = new ArrayDeque<>();
    this.lock = new ReentrantLock();
    this.notEmpty = this.lock.newCondition();
    this.notFull = this.lock.newCondition();
  }

  public Optional<Message<T>> publishMessage(final Message<T> message) {
    Objects.requireNonNull(message);
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      if (this.messageQueue.add(message)) {
        notEmpty.signal();
        return Optional.of(message);
      }
    } catch (Exception ex) {
      System.err.println("Error publishing message to queue: " + ex.getMessage());
      ex.printStackTrace();
    } finally {
      lock.unlock();
    }

    return Optional.empty();
  }

  public Optional<Message<T>> pollMessage(long timeout, TimeUnit unit) throws InterruptedException {
    long nanos = unit.toNanos(timeout);
    final ReentrantLock lock = this.lock;
    lock.lockInterruptibly();
    try {
      while (this.messageQueue.size() == 0) {
        if (nanos <= 0L) {
          return Optional.empty();
        }

        nanos = notEmpty.awaitNanos(nanos);
      }
      notFull.signal();
      return Optional.ofNullable(this.messageQueue.poll());
    } finally {
      lock.unlock();
    }
  }

  public String getName() {
    return name;
  }
}
