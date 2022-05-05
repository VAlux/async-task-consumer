package dev.alvo;

import dev.alvo.consumer.Broker;
import dev.alvo.consumer.MessageConsumer;

import java.util.concurrent.Executors;

public class Main {
  public static void main(String[] args) {
    final var broker = new Broker();

    final var stringMessageConsumer = new MessageConsumer<String>() {
      @Override
      public boolean consume(Message<String> message) {
        System.out.println(Thread.currentThread().getName() + " thread consumed STRING message: " + message.data());
        return true;
      }
    };

    final var intMessageConsumer = new MessageConsumer<Integer>() {
      @Override
      public boolean consume(Message<Integer> message) {
        System.out.println(Thread.currentThread().getName() + " thread consumed INTEGER message: " + message.data());
        return true;
      }
    };

    final var stringMessageProducer = new Producer<String>() {
      @Override
      public Message<String> produce() {
        return Message.of("String message " + Math.random());
      }
    };

    final var intMessageProducer = new Producer<Integer>() {
      @Override
      public Message<Integer> produce() {
        return Message.of((int) (Math.random() * 100));
      }
    };

    String stringTopicName = "string-topic";
    String intTopicName = "int-topic";

    broker.register(stringMessageConsumer, stringTopicName);
    broker.register(stringMessageConsumer, stringTopicName);
    broker.register(stringMessageConsumer, stringTopicName);
    broker.register(stringMessageConsumer, stringTopicName);

    broker.register(intMessageConsumer, intTopicName);
    broker.register(intMessageConsumer, intTopicName);
    broker.register(intMessageConsumer, intTopicName);

    final var producerExecutor = Executors.newFixedThreadPool(4);

    for (int i = 0; i < 4; i++) {
      producerExecutor.execute(() -> {
        for (int j = 0; j < 10_000; j++) {
          broker.publish(stringMessageProducer.produce(), stringTopicName);
          broker.publish(intMessageProducer.produce(), intTopicName);
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      });
    }
  }
}
