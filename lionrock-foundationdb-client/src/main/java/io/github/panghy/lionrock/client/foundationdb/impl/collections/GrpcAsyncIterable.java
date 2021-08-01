package io.github.panghy.lionrock.client.foundationdb.impl.collections;

import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import io.github.panghy.lionrock.proto.GetRangeResponse;
import io.github.panghy.lionrock.proto.OperationFailureResponse;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Implementation of {@link AsyncIterable} that handles the logic behind handling a single
 * {@link io.github.panghy.lionrock.proto.GetRangeRequest} with multiple {@link GetRangeResponse} streaming back to the
 * client.
 *
 * @author Clement Pang
 */
public class GrpcAsyncIterable<T, Resp> implements AsyncIterable<T> {

  private final GrpcAsyncIterator.RemovalCallback<T> removalCallback;
  private final Function<Resp, Stream<T>> respToStreamFunction;
  private final Function<Resp, Boolean> isDoneFunction;
  private final FetchIssuer<Resp> fetchIssuer;
  private final Executor executorService;

  public GrpcAsyncIterable(GrpcAsyncIterator.RemovalCallback<T> removalCallback,
                           Function<Resp, Stream<T>> respToStreamFunction,
                           Function<Resp, Boolean> isDoneFunction,
                           FetchIssuer<Resp> fetchIssuer,
                           Executor executorService) {
    this.removalCallback = removalCallback;
    this.respToStreamFunction = respToStreamFunction;
    this.isDoneFunction = isDoneFunction;
    this.fetchIssuer = fetchIssuer;
    this.executorService = executorService;
  }

  @Override
  public AsyncIterator<T> iterator() {
    GrpcAsyncIterator<T, Resp> grpcAsyncIterator = new GrpcAsyncIterator<>(
        removalCallback, respToStreamFunction, isDoneFunction, executorService);
    this.fetchIssuer.issue(grpcAsyncIterator::accept, grpcAsyncIterator::accept);
    return grpcAsyncIterator;
  }

  @Override
  public CompletableFuture<List<T>> asList() {
    return AsyncUtil.collect(this, executorService);
  }

  /**
   * Interface to the active transaction to issue a fetch.
   */
  public interface FetchIssuer<Resp> {
    /**
     * Issue a request with the given callback intending to receive responses for this iterable.
     *
     * @param responseConsumer Consumer of responses.
     * @param failureConsumer  Consumer of failures.
     */
    void issue(Consumer<Resp> responseConsumer, Consumer<OperationFailureResponse> failureConsumer);
  }
}
