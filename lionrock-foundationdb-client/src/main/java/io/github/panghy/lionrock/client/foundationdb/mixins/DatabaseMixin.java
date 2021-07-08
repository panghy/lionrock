package io.github.panghy.lionrock.client.foundationdb.mixins;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * {@link Database} mix-in to allow for named transactions (for better tracing and debugging) optionally.
 *
 * @author Clement Pang
 */
public interface DatabaseMixin extends Database {

  @Override
  default <T> T read(Function<? super ReadTransaction, T> retryable, Executor e) {
    return this.run(retryable, e);
  }

  @Override
  default <T> CompletableFuture<T> readAsync(
      Function<? super ReadTransaction, ? extends CompletableFuture<T>> retryable, Executor e) {
    return this.runAsync(retryable, e);
  }

  @Override
  default <T> CompletableFuture<T> runAsync(Function<? super Transaction, ? extends CompletableFuture<T>> retryable,
                                            Executor e) {
    return runAsync("unnamed_run_async_transaction", retryable, e);
  }

  @Override
  default <T> T run(Function<? super Transaction, T> retryable, Executor e) {
    return run("unnamed_run_transaction", retryable, e);
  }

  default <T> T read(String name, Function<? super ReadTransaction, T> retryable, Executor e) {
    return this.run(name, retryable, e);
  }

  default <T> CompletableFuture<T> readAsync(
      String name, Function<? super ReadTransaction, ? extends CompletableFuture<T>> retryable, Executor e) {
    return this.runAsync(name, retryable, e);
  }

  <T> T run(String name, Function<? super Transaction, T> retryable, Executor e);

  <T> CompletableFuture<T> runAsync(
      String name, Function<? super Transaction, ? extends CompletableFuture<T>> retryable, Executor e);
}
