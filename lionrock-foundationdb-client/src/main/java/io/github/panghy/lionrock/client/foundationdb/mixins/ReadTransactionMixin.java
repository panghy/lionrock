package io.github.panghy.lionrock.client.foundationdb.mixins;

import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * An mix-in implementation of {@link Transaction} with some functions implemented as a convenience.
 *
 * @author Clement Pang
 */
public interface ReadTransactionMixin extends ReadTransaction {

  /**
   * The prefix under which all FDB system and special data live. Regular user operations typically cannot read keys
   * with this prefix unless they set special options except in special circumstances.
   */
  byte[] SYSTEM_PREFIX = {(byte) 0xff};

  byte[] METADATA_VERSION_KEY = systemPrefixedKey("/metadataVersion");

  private static byte[] systemPrefixedKey(@Nonnull String key) {
    return ByteArrayUtil.join(SYSTEM_PREFIX, key.getBytes(StandardCharsets.US_ASCII));
  }

  ///////////////////
  //  getRange -> KeySelectors
  ///////////////////

  @Override
  default AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end,
                                           int limit, boolean reverse) {
    return getRange(begin, end, limit, reverse, StreamingMode.ITERATOR);
  }

  @Override
  default AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end,
                                           int limit) {
    return getRange(begin, end, limit, false);
  }

  @Override
  default AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end) {
    return getRange(begin, end, ReadTransaction.ROW_LIMIT_UNLIMITED);
  }

  ///////////////////
  //  getRange -> byte[]s
  ///////////////////

  @Override
  default AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end,
                                           int limit, boolean reverse, StreamingMode mode) {
    return getRange(KeySelector.firstGreaterOrEqual(begin),
        KeySelector.firstGreaterOrEqual(end),
        limit, reverse, mode);
  }

  @Override
  default AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end,
                                           int limit, boolean reverse) {
    return getRange(begin, end, limit, reverse, StreamingMode.ITERATOR);
  }

  @Override
  default AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end,
                                           int limit) {
    return getRange(begin, end, limit, false);
  }

  @Override
  default AsyncIterable<KeyValue> getRange(byte[] begin, byte[] end) {
    return getRange(begin, end, ReadTransaction.ROW_LIMIT_UNLIMITED);
  }

  ///////////////////
  //  getRange (Range)
  ///////////////////
  @Override
  default AsyncIterable<KeyValue> getRange(Range range,
                                           int limit, boolean reverse, StreamingMode mode) {
    return getRange(range.begin, range.end, limit, reverse, mode);
  }

  @Override
  default AsyncIterable<KeyValue> getRange(Range range,
                                           int limit, boolean reverse) {
    return getRange(range, limit, reverse, StreamingMode.ITERATOR);
  }

  @Override
  default AsyncIterable<KeyValue> getRange(Range range,
                                           int limit) {
    return getRange(range, limit, false);
  }

  @Override
  default AsyncIterable<KeyValue> getRange(Range range) {
    return getRange(range, ReadTransaction.ROW_LIMIT_UNLIMITED);
  }

  @Override
  default <T> T read(Function<? super ReadTransaction, T> retryable) {
    return retryable.apply(this);
  }

  @Override
  default <T> CompletableFuture<T> readAsync(Function<? super ReadTransaction, ? extends CompletableFuture<T>> retryable) {
    return AsyncUtil.applySafely(retryable, this);
  }
}
