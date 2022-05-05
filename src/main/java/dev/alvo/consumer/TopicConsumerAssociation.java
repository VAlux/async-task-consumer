package dev.alvo.consumer;

import dev.alvo.Message;
import dev.alvo.Topic;

import java.util.List;
import java.util.Optional;

public class TopicConsumerAssociation<T> {

  private final Topic<T> topic;
  private final List<? extends MessageConsumer<T>> consumers;
  private final Class<T> dataType;
  private final MessageQueueProcessor<T> processor;

  public TopicConsumerAssociation(Topic<T> topic, List<? extends MessageConsumer<T>> consumers, Class<T> dataType) {
    this.topic = topic;
    this.consumers = consumers;
    this.dataType = dataType;
    this.processor = new MessageQueueProcessor<>(Runtime.getRuntime().availableProcessors(), this);
  }

  public Optional<Message<T>> publish(final Message<T> message) {
    return topic.publishMessage(message);
  }

  public Topic<T> getTopic() {
    return topic;
  }

  public List<? extends MessageConsumer<T>> getConsumers() {
    return consumers;
  }

  public Class<T> getDataType() {
    return dataType;
  }

  public MessageQueueProcessor<T> getProcessor() {
    return processor;
  }
}
