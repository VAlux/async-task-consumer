package dev.alvo.consumer;

import dev.alvo.Message;

public interface MessageConsumer<T> {
  Class<T> consumableDataType();

  boolean consume(final Message<T> message);
}
