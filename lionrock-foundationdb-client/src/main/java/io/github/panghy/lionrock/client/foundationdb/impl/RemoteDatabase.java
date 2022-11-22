package io.github.panghy.lionrock.client.foundationdb.impl;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.DatabaseOptions;
import com.apple.foundationdb.EventKeeper;
import com.apple.foundationdb.Transaction;
import io.github.panghy.lionrock.client.foundationdb.mixins.DatabaseMixin;
import io.github.panghy.lionrock.proto.TransactionalKeyValueStoreGrpc;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.apple.foundationdb.async.AsyncUtil.*;

/**
 * A {@link Database} that is connected via gRPC to a remote instance of a lionrock server (could be ultimately backed
 * by FoundationDB).
 *
 * @author Clement Pang
 */
public class RemoteDatabase implements DatabaseMixin {

  private final String databaseName;
  private final String clientIdentifier;
  private final Executor executorService;
  private final TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub;
  /**
   * Unused for now.
   */
  private final DatabaseOptions databaseOptions = new DatabaseOptions((code, parameter) -> {
  });

  public RemoteDatabase(String databaseName,
                        String clientIdentifier,
                        Executor executorService,
                        TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub) {
    this.databaseName = databaseName;
    this.clientIdentifier = clientIdentifier;
    this.executorService = executorService;
    this.stub = stub;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getClientIdentifier() {
    return clientIdentifier;
  }

  public Transaction createTransaction(String name, Executor e) {
    long start = System.currentTimeMillis();
    return new RemoteTransaction(new RemoteTransaction.RemoteTransactionContext() {
      final AtomicLong attempts = new AtomicLong(0);

      long timeoutMs = -1;
      long maxAttempts = -1;
      long maxRetryDelayMs = 1000;

      @Override
      public String getName() {
        return name;
      }

      @Override
      public Executor getExecutor() {
        return e;
      }

      @Override
      public TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub getStub() {
        return stub;
      }

      @Override
      public RemoteDatabase getDatabase() {
        return RemoteDatabase.this;
      }

      @Override
      public long getAttempts() {
        return this.attempts.get();
      }

      @Override
      public void incrementAttempts() {
        this.attempts.incrementAndGet();
      }

      @Override
      public long getMillisecondsLeft() {
        if (timeoutMs < 0) {
          return Long.MAX_VALUE;
        }
        return Math.max(0, timeoutMs - (System.currentTimeMillis() - start));
      }

      @Override
      public long getAttemptsLeft() {
        if (maxAttempts < 0) {
          return Long.MAX_VALUE;
        }
        return Math.max(0, maxAttempts - attempts.get());
      }

      @Override
      public long getMaxRetryDelayMs() {
        return maxRetryDelayMs;
      }

      @Override
      public void setTimeout(long timeoutMs) {
        this.timeoutMs = timeoutMs;
      }

      @Override
      public void setMaxRetryDelay(long delayMs) {
        this.maxRetryDelayMs = delayMs;
      }

      @Override
      public void setRetryLimit(long limit) {
        this.maxAttempts = limit;
      }
    }, 60_000);
  }

  @Override
  public Transaction createTransaction(Executor e) {
    return createTransaction("unnamed_transaction", e);
  }

  /**
   * Ignores {@link EventKeeper} at this point.
   */
  @Override
  public Transaction createTransaction(Executor e, EventKeeper eventKeeper) {
    return createTransaction(e);
  }

  @Override
  public DatabaseOptions options() {
    return databaseOptions;
  }

  public <T> T run(String name, Function<? super Transaction, T> retryable, Executor e) {
    Transaction t = this.createTransaction(name, e);
    try {
      while (true) {
        try {
          T returnVal = retryable.apply(t);
          t.commit().join();
          return returnVal;
        } catch (RuntimeException err) {
          t = t.onError(err).join();
        }
      }
    } finally {
      t.close();
    }
  }

  @Override
  public <T> CompletableFuture<T> runAsync(
      String name, Function<? super Transaction, ? extends CompletableFuture<T>> retryable, Executor e) {
    final AtomicReference<Transaction> trRef = new AtomicReference<>(createTransaction(name, e));
    final AtomicReference<T> returnValue = new AtomicReference<>();
    return whileTrue(() ->
        composeHandleAsync(
            applySafely(retryable, trRef.get()).
                thenComposeAsync(returnVal ->
                    trRef.get().commit().thenApply(o -> {
                      returnValue.set(returnVal);
                      return false;
                    }), e),
            (value, t) -> {
              if (t == null) {
                return CompletableFuture.completedFuture(value);
              }
              if (!(t instanceof RuntimeException)) {
                throw new CompletionException(t);
              }
              return trRef.get().onError(t).thenApply(newTr -> {
                trRef.set(newTr);
                return true;
              });
            }, e), e).
        thenApply(o -> returnValue.get()).
        whenComplete((v, t) -> trRef.get().close());
  }

  @Override
  public void close() {
    // ignored.
  }

  @Override
  public Executor getExecutor() {
    return executorService;
  }
}
