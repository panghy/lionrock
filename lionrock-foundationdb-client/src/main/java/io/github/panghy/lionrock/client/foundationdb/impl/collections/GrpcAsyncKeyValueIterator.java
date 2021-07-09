package io.github.panghy.lionrock.client.foundationdb.impl.collections;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import io.github.panghy.lionrock.proto.GetRangeResponse;
import io.github.panghy.lionrock.proto.OperationFailureResponse;

import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

/**
 * An {@link AsyncIterator} backed by a gRPC call to a transaction (with streaming results coming back via the object
 * {@link io.github.panghy.lionrock.proto.GetRangeResponse}.
 *
 * @author Clement Pang
 */
public class GrpcAsyncKeyValueIterator implements AsyncIterator<KeyValue> {

  private final RemovalCallback removalCallback;
  /**
   * {@link LinkedList} to store {@link GetRangeResponse}s.
   */
  private final LinkedList<KeyValue> responses = new LinkedList<>();
  /**
   * {@link CompletableFuture} to signal when a result is available.
   */
  private CompletableFuture<Boolean> onHasNextFuture = new CompletableFuture<>();
  private boolean done = false;
  private boolean cancelled = false;
  private byte[] prevKey = null;

  public GrpcAsyncKeyValueIterator(RemovalCallback removalCallback) {
    this.removalCallback = removalCallback;
  }

  void accept(GetRangeResponse resp) {
    if (onHasNextFuture.isCompletedExceptionally()) {
      // eat the response if we have failed already.
      return;
    }
    if (resp.getDone()) {
      done = true;
    }
    synchronized (responses) {
      boolean wasEmpty = responses.isEmpty();
      // just adding more keys.
      resp.getKeyValuesList().stream().
          map(kv -> new KeyValue(kv.getKey().toByteArray(), kv.getValue().toByteArray())).
          forEach(responses::add);
      if (wasEmpty) {
        // waiting for key values.
        if (resp.getDone() && responses.isEmpty()) {
          onHasNextFuture.complete(false);
        } else {
          onHasNextFuture.complete(true);
        }
      }
    }
  }

  void accept(OperationFailureResponse failure) {
    FDBException ex = new FDBException(failure.getMessage(), (int) failure.getCode());
    if (!onHasNextFuture.completeExceptionally(ex)) {
      onHasNextFuture = CompletableFuture.failedFuture(ex);
    }
  }

  @Override
  public CompletableFuture<Boolean> onHasNext() {
    if (cancelled) {
      throw new CancellationException();
    }
    if (!responses.isEmpty()) {
      // has keys pending to be iterated.
      return AsyncUtil.READY_TRUE;
    } else if (onHasNextFuture != null &&
        (!onHasNextFuture.isDone() || onHasNextFuture.isCompletedExceptionally())) {
      // already has a future and it isn't done.
      return onHasNextFuture;
    } else if (done) {
      // already done and no responses left.
      return AsyncUtil.READY_FALSE;
    }
    synchronized (responses) {
      if (responses.isEmpty() &&
          (onHasNextFuture == null ||
              (onHasNextFuture.isDone() &&
                  !onHasNextFuture.isCompletedExceptionally()))) {
        onHasNextFuture = new CompletableFuture<>();
      }
    }
    return onHasNextFuture;
  }

  @Override
  public boolean hasNext() {
    if (cancelled) {
      throw new CancellationException();
    }
    if (!responses.isEmpty()) {
      return true;
    } else if (done) {
      return false;
    }
    return onHasNextFuture.join();
  }

  @Override
  public KeyValue next() {
    if (cancelled) {
      throw new CancellationException();
    }
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    synchronized (responses) {
      KeyValue keyValue = responses.removeFirst();
      prevKey = keyValue.getKey();
      return keyValue;
    }
  }

  @Override
  public void cancel() {
    cancelled = true;
  }

  @Override
  public void remove() {
    if (prevKey == null) {
      throw new IllegalStateException("No value has been fetched from database");
    }
    removalCallback.deleteKey(prevKey);
  }

  /**
   * Interface supplied to {@link GrpcAsyncKeyValueIterator} to delete a key during iteration.
   */
  public interface RemovalCallback {
    void deleteKey(byte[] key);
  }
}
