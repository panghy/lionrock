package io.github.panghy.lionrock.client.foundationdb.impl.collections;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import io.github.panghy.lionrock.proto.GetRangeResponse;
import io.github.panghy.lionrock.proto.OperationFailureResponse;

import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * An {@link AsyncIterator} backed by a gRPC call to a transaction (with streaming results coming back via the object
 * {@link io.github.panghy.lionrock.proto.GetRangeResponse}.
 *
 * @author Clement Pang
 */
public class GrpcAsyncIterator<T, Resp> implements AsyncIterator<T> {

  private final RemovalCallback<T> removalCallback;
  /**
   * {@link LinkedList} to store {@link GetRangeResponse}s.
   */
  private final LinkedList<T> responses = new LinkedList<>();
  /**
   * {@link CompletableFuture} to signal when a result is available.
   */
  private CompletableFuture<Boolean> onHasNextFuture;
  private boolean done = false;
  private boolean cancelled = false;
  private T prev = null;

  private final Function<Resp, Stream<T>> respToStreamFunction;
  private Function<String, CompletableFuture<Boolean>> completableFutureSupplier;
  private final Function<Resp, Boolean> isDoneFunction;
  private final Executor executor;

  public GrpcAsyncIterator(RemovalCallback removalCallback,
                           Function<Resp, Stream<T>> respToStreamFunction,
                           Function<String, CompletableFuture<Boolean>> completableFutureSupplier,
                           Function<Resp, Boolean> isDoneFunction, Executor executor) {
    this.removalCallback = removalCallback;
    this.respToStreamFunction = respToStreamFunction;
    this.completableFutureSupplier = completableFutureSupplier;
    this.onHasNextFuture = completableFutureSupplier.apply("GrpcAsyncIterator.onHasNext()");
    this.isDoneFunction = isDoneFunction;
    this.executor = executor;
  }

  void accept(Resp resp) {
    if (onHasNextFuture.isCompletedExceptionally()) {
      // eat the response if we have failed already.
      return;
    }
    synchronized (responses) {
      if (isDoneFunction.apply(resp)) {
        done = true;
      }
      boolean wasEmpty = responses.isEmpty();
      // just adding more keys.
      respToStreamFunction.apply(resp).
          forEach(responses::add);
      if (wasEmpty) {
        // waiting for key values.
        if (isDoneFunction.apply(resp) && responses.isEmpty()) {
          onHasNextFuture.completeAsync(() -> false, executor);
        } else if (!responses.isEmpty()) {
          onHasNextFuture.completeAsync(() -> true, executor);
        }
      }
    }
  }

  void accept(OperationFailureResponse failure) {
    FDBException ex = new FDBException(failure.getMessage(), (int) failure.getCode());
    synchronized (responses) {
      if (!onHasNextFuture.completeExceptionally(ex)) {
        onHasNextFuture = CompletableFuture.failedFuture(ex);
      }
    }
  }

  @Override
  public CompletableFuture<Boolean> onHasNext() {
    if (cancelled) {
      throw new CancellationException();
    }
    synchronized (responses) {
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
      if (responses.isEmpty() &&
          (onHasNextFuture == null ||
              (onHasNextFuture.isDone() &&
                  !onHasNextFuture.isCompletedExceptionally()))) {
        onHasNextFuture = completableFutureSupplier.apply("GrpcAsyncIterator.onHasNext()");
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
    return onHasNext().join();
  }

  @Override
  public T next() {
    if (cancelled) {
      throw new CancellationException();
    }
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    synchronized (responses) {
      T keyValue = responses.removeFirst();
      prev = keyValue;
      return keyValue;
    }
  }

  @Override
  public void cancel() {
    cancelled = true;
  }

  @Override
  public void remove() {
    if (prev == null) {
      throw new IllegalStateException("No value has been fetched from database");
    }
    removalCallback.deleteKey(prev);
  }

  /**
   * Interface supplied to {@link GrpcAsyncIterator} to delete a key during iteration.
   */
  public interface RemovalCallback<T> {
    void deleteKey(T key);
  }
}
