package dev.alvo;

@FunctionalInterface
public interface Producer<T> {
  Message<T> produce();
}
