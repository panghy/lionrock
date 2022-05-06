package io.github.panghy.lionrock.inmemory;

import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.apple.foundationdb.tuple.ByteArrayUtil.printable;
import static com.apple.foundationdb.tuple.ByteArrayUtil.strinc;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Implements a pseudo FoundationDB database.
 *
 * @author Clement Pang.
 */
public class InmemoryDatabase extends TreeMap<BytesKey, BytesValue> implements Database {

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
  private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

  private final DatabaseOptions databaseOptions = new DatabaseOptions((code, parameter) -> {
    // do nothing.
  });

  @Override
  public Transaction createTransaction(Executor e) {
    return createTransaction(e, null);
  }

  @Override
  public Transaction createTransaction(Executor e, EventKeeper eventKeeper) {
    writeLock.lock();
    return new InmemoryTransaction();
  }

  @Override
  public DatabaseOptions options() {
    return databaseOptions;
  }

  @Override
  public <T> T read(Function<? super ReadTransaction, T> retryable, Executor e) {
    Transaction transaction = createTransaction();
    try {
      return retryable.apply(transaction);
    } finally {
      transaction.commit();
    }
  }

  @Override
  public <T> CompletableFuture<T> readAsync(Function<? super ReadTransaction, ? extends CompletableFuture<T>> retryable, Executor e) {
    Transaction transaction = createTransaction();
    try {
      return retryable.apply(transaction);
    } finally {
      transaction.commit();
    }
  }

  @Override
  public <T> T run(Function<? super Transaction, T> retryable, Executor e) {
    Transaction transaction = createTransaction();
    try {
      return retryable.apply(transaction);
    } finally {
      transaction.commit();
    }
  }

  @Override
  public <T> CompletableFuture<T> runAsync(Function<? super Transaction, ? extends CompletableFuture<T>> retryable, Executor e) {
    Transaction transaction = createTransaction();
    try {
      return retryable.apply(transaction);
    } finally {
      transaction.commit();
    }
  }

  @Override
  public void close() {
  }

  @Override
  public Executor getExecutor() {
    return null;
  }

  public class InmemoryTransaction implements Transaction {

    private final TransactionOptions transactionOptions = new TransactionOptions(new OptionConsumer() {
      @Override
      public void setOption(int code, byte[] parameter) {
        // do nothing.
      }
    });

    @Override
    public void addReadConflictRange(byte[] keyBegin, byte[] keyEnd) {

    }

    @Override
    public void addReadConflictKey(byte[] key) {

    }

    @Override
    public void addWriteConflictRange(byte[] keyBegin, byte[] keyEnd) {

    }

    @Override
    public void addWriteConflictKey(byte[] key) {

    }

    @Override
    public void set(byte[] key, byte[] value) {
      put(new BytesKey(key), new BytesValue(value));
    }

    @Override
    public void clear(byte[] key) {
      remove(new BytesKey(key));
    }

    @Override
    public void clear(byte[] beginKey, byte[] endKey) {
      subMap(new BytesKey(beginKey), new BytesKey(endKey)).clear();
    }

    @Override
    public void clear(Range range) {
      subMap(new BytesKey(range.begin), new BytesKey(range.end)).clear();
    }

    @Override
    public void clearRangeStartsWith(byte[] prefix) {
      subMap(new BytesKey(prefix), new BytesKey(strinc(prefix))).clear();
    }

    @Override
    public void mutate(MutationType optype, byte[] key, byte[] param) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> commit() {
      writeLock.unlock();
      return completedFuture(null);
    }

    @Override
    public Long getCommittedVersion() {
      return 1L;
    }

    @Override
    public CompletableFuture<byte[]> getVersionstamp() {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Long> getApproximateSize() {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Transaction> onError(Throwable e) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void cancel() {
    }

    @Override
    public CompletableFuture<Void> watch(byte[] key) throws FDBException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Database getDatabase() {
      return InmemoryDatabase.this;
    }

    @Override
    public <T> T run(Function<? super Transaction, T> retryable) {
      return retryable.apply(this);
    }

    @Override
    public <T> CompletableFuture<T> runAsync(Function<? super Transaction, ? extends CompletableFuture<T>> retryable) {
      return retryable.apply(this);
    }

    @Override
    public void close() {
    }

    @Override
    public boolean isSnapshot() {
      return false;
    }

    @Override
    public ReadTransaction snapshot() {
      return this;
    }

    @Override
    public CompletableFuture<Long> getReadVersion() {
      return completedFuture(1L);
    }

    @Override
    public void setReadVersion(long version) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean addReadConflictRangeIfNotSnapshot(byte[] keyBegin, byte[] keyEnd) {
      return false;
    }

    @Override
    public boolean addReadConflictKeyIfNotSnapshot(byte[] key) {
      return false;
    }

    @Override
    public CompletableFuture<byte[]> get(byte[] key) {
      BytesValue bytesValue = InmemoryDatabase.this.get(new BytesKey(key));
      if (bytesValue == null) {
        return completedFuture(null);
      }
      return completedFuture(bytesValue.getValue());
    }

    @Override
    public CompletableFuture<byte[]> getKey(KeySelector selector) {
      return completedFuture(resolveKeySelector(selector).getValue());
    }

    @Override
    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end) {
      return getRange(begin, end, ROW_LIMIT_UNLIMITED);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, int limit) {
      return getRange(begin, end, limit, false);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, int limit, boolean reverse) {
      return getRange(begin, end, limit, reverse, StreamingMode.EXACT);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, int limit, boolean reverse, StreamingMode mode) {
      BytesKey startBK = resolveKeySelector(begin);
      BytesKey endBK = resolveKeySelector(end);
      SortedMap<BytesKey, BytesValue> subMap = reverse ? descendingMap().subMap(startBK, endBK) :
          subMap(startBK, endBK);
      Iterable<Map.Entry<BytesKey, BytesValue>> iterable = subMap.entrySet();
      if (limit > 0) {
        iterable = Iterables.limit(iterable, limit);
      }
      return getAsyncIterable(iterable);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end) {
      return getRange(begin, end, ROW_LIMIT_UNLIMITED);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end, int limit) {
      return getRange(begin, end, limit, false);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end, int limit, boolean reverse) {
      return getRange(begin, end, limit, reverse, StreamingMode.EXACT);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end, int limit, boolean reverse, StreamingMode mode) {
      SortedMap<BytesKey, BytesValue> subMap = reverse ?
          descendingMap().subMap(new BytesKey(begin), new BytesKey(end)) :
          subMap(new BytesKey(begin), new BytesKey(end));
      Iterable<Map.Entry<BytesKey, BytesValue>> iterable = subMap.entrySet();
      if (limit > 0) {
        iterable = Iterables.limit(iterable, limit);
      }
      return getAsyncIterable(iterable);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(Range range) {
      return getRange(range, ROW_LIMIT_UNLIMITED);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(Range range, int limit) {
      return getRange(range, limit, false);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(Range range, int limit, boolean reverse) {
      return getRange(range, limit, reverse, StreamingMode.EXACT);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(Range range, int limit, boolean reverse, StreamingMode mode) {
      return getRange(range.begin, range.end, limit, reverse, mode);
    }

    @Override
    public CompletableFuture<Long> getEstimatedRangeSizeBytes(byte[] begin, byte[] end) {
      SortedMap<BytesKey, BytesValue> sub = subMap(new BytesKey(begin), new BytesKey(end));
      return completedFuture(
          sub.entrySet().stream().
              mapToLong(kv -> kv.getKey().getValue().length + kv.getValue().getValue().length).
              sum());
    }

    @Override
    public CompletableFuture<Long> getEstimatedRangeSizeBytes(Range range) {
      return getEstimatedRangeSizeBytes(range.begin, range.end);
    }

    @Override
    public TransactionOptions options() {
      return transactionOptions;
    }

    @Override
    public <T> T read(Function<? super ReadTransaction, T> retryable) {
      return retryable.apply(this);
    }

    @Override
    public <T> CompletableFuture<T> readAsync(Function<? super ReadTransaction, ? extends CompletableFuture<T>> retryable) {
      return retryable.apply(this);
    }

    @Override
    public Executor getExecutor() {
      return null;
    }

    private AsyncIterable<KeyValue> getAsyncIterable(Iterable<Map.Entry<BytesKey, BytesValue>> finalIterable) {
      return new AsyncIterable<>() {
        @Override
        public AsyncIterator<KeyValue> iterator() {
          Iterator<Map.Entry<BytesKey, BytesValue>> iterator = finalIterable.iterator();
          return new AsyncIterator<KeyValue>() {
            @Override
            public CompletableFuture<Boolean> onHasNext() {
              return completedFuture(hasNext());
            }

            @Override
            public boolean hasNext() {
              return iterator.hasNext();
            }

            @Override
            public KeyValue next() {
              Map.Entry<BytesKey, BytesValue> next = iterator.next();
              return new KeyValue(next.getKey().getValue(), next.getValue().getValue());
            }

            @Override
            public void cancel() {
            }
          };
        }

        @Override
        public CompletableFuture<List<KeyValue>> asList() {
          return completedFuture(Streams.stream(finalIterable.iterator()).
              map(kv -> new KeyValue(kv.getKey().getValue(), kv.getValue().getValue())).
              collect(Collectors.toList()));
        }
      };
    }
  }

  /**
   * The way FDB key selectors work is not quite intuitive. See {@link KeySelector}.
   */
  @Nonnull
  private BytesKey resolveKeySelector(KeySelector selector) {
    boolean orEquals = orEquals(selector);
    byte[] key = selector.getKey();
    int offset = selector.getOffset();
    if (offset > 0) {
      NavigableMap<BytesKey, BytesValue> map = tailMap(new BytesKey(key), !orEquals);
      BytesKey bytesKey = Iterables.get(map.keySet(), offset - 1);
      if (bytesKey == null) {
        return new BytesKey(new byte[]{(byte) 255});
      }
      return bytesKey;
    } else {
      // offset of zero moves backwards.
      NavigableMap<BytesKey, BytesValue> map = headMap(new BytesKey(key), orEquals);
      BytesKey bytesKey = Iterables.get(map.descendingKeySet(), offset);
      if (bytesKey == null) {
        return new BytesKey(new byte[0]);
      }
      return bytesKey;
    }
  }

  private static boolean orEquals(KeySelector keySelector) {
    String kSStr = keySelector.toString();
    byte[] key = keySelector.getKey();
    String printableKey = printable(key);
    String orEqualsTrue = String.format("(%s, %s, %d)", printableKey, true, keySelector.getOffset());
    return kSStr.equals(orEqualsTrue);
  }
}
