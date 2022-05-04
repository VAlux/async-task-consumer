package dev.alvo.consumer;

import dev.alvo.Message;
import dev.alvo.Topic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class Broker {

  private final Map<String, TopicConsumerAssociation<?>> registry;

  public Broker() {
    this.registry = new HashMap<>();
  }

  @SuppressWarnings("unchecked")
  public <T> void register(final MessageConsumer<T> consumer, String topicName) {
    final TopicConsumerAssociation<?> existingTopic = registry.get(topicName);
    if (existingTopic != null) {
      if (existingTopic.getDataType().equals(consumer.consumableDataType())) {
        ((List<MessageConsumer<T>>) existingTopic.getConsumers()).add(consumer);
      } else {
        throw new RuntimeException("This topic can't handle messages of the corresponding type");
      }
    } else {
      registry.put(
          topicName,
          new TopicConsumerAssociation<>(
              new Topic<>(topicName),
              new ArrayList<>(List.of(consumer)),
              consumer.consumableDataType()));
    }
  }

  @SuppressWarnings("unchecked")
  public <T> Optional<Message<T>> publish(final Message<T> message, final String topicName) {
    try {
      final var association = ((TopicConsumerAssociation<T>) this.registry.get(topicName));
      if (association != null && message.data().getClass().equals(association.getDataType())) {
        final var publishedMessage = association.publish(message);
        association.getProcessor().launchProcessing();
        return publishedMessage;
      } else {
        System.err.println("Can't publish the message to topic " + topicName);
        return Optional.empty();
      }
    } catch (Exception ex) {
      System.err.println("Can't publish to message to topic " + topicName + " because: " + ex.getMessage());
      ex.printStackTrace();
      return Optional.empty();
    }
  }
}
