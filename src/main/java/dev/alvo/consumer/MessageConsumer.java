package dev.alvo.consumer;

import dev.alvo.Message;

import java.lang.reflect.ParameterizedType;

public abstract class MessageConsumer<T> {

  @SuppressWarnings("unchecked")
  private final Class<T> consumableDataType =
      (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];

  public Class<T> getConsumableDataType() {
    return consumableDataType;
  }

  public abstract boolean consume(final Message<T> message);
}
