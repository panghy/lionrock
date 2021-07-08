package io.github.panghy.lionrock.client.foundationdb.impl.collections;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import io.github.panghy.lionrock.client.foundationdb.impl.StreamingDatabaseResponseVisitor;
import io.github.panghy.lionrock.proto.GetRangeResponse;
import io.github.panghy.lionrock.proto.OperationFailureResponse;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Implementation of {@link AsyncIterable} that handles the logic behind handling a single
 * {@link io.github.panghy.lionrock.proto.GetRangeRequest} with multiple {@link GetRangeResponse} streaming back to the
 * client.
 *
 * @author Clement Pang
 */
public class GrpcAsyncKeyValueIterable implements AsyncIterable<KeyValue> {

  private final GrpcAsyncKeyValueIterator.RemovalCallback removalCallback;
  private final GetRangeSupplier getRangeSupplier;
  private final Executor executorService;

  public GrpcAsyncKeyValueIterable(GrpcAsyncKeyValueIterator.RemovalCallback removalCallback,
                                   GetRangeSupplier getRangeSupplier,
                                   Executor executorService) {
    this.removalCallback = removalCallback;
    this.getRangeSupplier = getRangeSupplier;
    this.executorService = executorService;
  }

  @Override
  public AsyncIterator<KeyValue> iterator() {
    GrpcAsyncKeyValueIterator grpcAsyncKeyValueIterator = new GrpcAsyncKeyValueIterator(removalCallback);
    this.getRangeSupplier.issueGetRange(new StreamingDatabaseResponseVisitor() {
      @Override
      public void handleGetRange(GetRangeResponse resp) {
        grpcAsyncKeyValueIterator.accept(resp);
      }

      @Override
      public void handleOperationFailure(OperationFailureResponse resp) {
        grpcAsyncKeyValueIterator.accept(resp);
      }
    });
    return grpcAsyncKeyValueIterator;
  }

  @Override
  public CompletableFuture<List<KeyValue>> asList() {
    return AsyncUtil.collect(this, executorService);
  }

  /**
   * Interface to the active transaction to issue a get range call.
   */
  public interface GetRangeSupplier {
    /**
     * Issue a get range request with the given callback intending to receive responses for the this iterable.
     *
     * @param callback Callback to handle results.
     */
    void issueGetRange(StreamingDatabaseResponseVisitor callback);
  }
}
