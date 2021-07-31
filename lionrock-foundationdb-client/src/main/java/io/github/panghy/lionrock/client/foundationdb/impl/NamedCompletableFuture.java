package io.github.panghy.lionrock.client.foundationdb.impl;

import com.google.common.base.Joiner;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Make {@link CompletableFuture} easier to debug.
 *
 * @param <T> Parameter type for the completable future.
 */
public class NamedCompletableFuture<T> extends CompletableFuture<T> {

  private final String name;
  private volatile Map<String, String> tags = null;

  public NamedCompletableFuture(String name) {
    this.name = name;
  }

  public String toString() {
    if (tags == null) {
      return name + ": " + super.toString();
    }
    return name + " [" + Joiner.on(",").withKeyValueSeparator("=").join(tags) + "]: " + super.toString();
  }

  public synchronized void addBaggage(String key, String value) {
    if (tags == null) {
      tags = new ConcurrentHashMap<>();
    }
    tags.put(key, value);
  }
}
