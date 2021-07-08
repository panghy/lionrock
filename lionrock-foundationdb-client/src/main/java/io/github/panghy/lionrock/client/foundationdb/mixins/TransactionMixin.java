package io.github.panghy.lionrock.client.foundationdb.mixins;

import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * An mix-in implementation of {@link Transaction} with some functions implemented as a convenience.
 *
 * @author Clement Pang
 */
public interface TransactionMixin extends ReadTransactionMixin, Transaction {

  @Override
  default boolean addReadConflictRangeIfNotSnapshot(byte[] keyBegin, byte[] keyEnd) {
    addReadConflictRange(keyBegin, keyEnd);
    return true;
  }

  @Override
  default boolean addReadConflictKeyIfNotSnapshot(byte[] key) {
    addReadConflictKey(key);
    return true;
  }

  @Override
  default void addReadConflictKey(byte[] key) {
    addReadConflictRange(key, ByteArrayUtil.join(key, new byte[]{(byte) 0}));
  }

  @Override
  default void addWriteConflictKey(byte[] key) {
    addWriteConflictRange(key, ByteArrayUtil.join(key, new byte[]{(byte) 0}));
  }

  @Override
  default void clear(Range range) {
    clear(range.begin, range.end);
  }

  @Override
  default void clearRangeStartsWith(byte[] prefix) {
    clear(Range.startsWith(prefix));
  }

  @Override
  default <T> T run(Function<? super Transaction, T> retryable) {
    return retryable.apply(this);
  }

  @Override
  default <T> CompletableFuture<T> runAsync(Function<? super Transaction, ? extends CompletableFuture<T>> retryable) {
    return AsyncUtil.applySafely(retryable, this);
  }
}
