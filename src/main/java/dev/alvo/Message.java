package dev.alvo;

public record Message<T>(T data) {
  public static <T> Message<T> of(final T data) {
    return new Message<>(data);
  }
}
