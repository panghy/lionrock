package io.github.panghy.lionrock.client.foundationdb.impl;

import io.github.panghy.lionrock.proto.StreamingDatabaseResponse;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executor;

import static java.util.Optional.ofNullable;

/**
 * Takes a {@link io.github.panghy.lionrock.proto.StreamingDatabaseResponse} and distributes the response to a number
 * of clients.
 *
 * @author Clement Pang
 */
public class SequenceResponseDemuxer {

  private final ConcurrentMap<Long, Set<StreamingDatabaseResponseVisitor>> sequenceResponseVisitors =
      new ConcurrentHashMap<>();
  private final Executor executor;

  public SequenceResponseDemuxer(Executor executor) {
    this.executor = executor;
  }

  void addHandler(long sequenceId, StreamingDatabaseResponseVisitor visitor) {
    sequenceResponseVisitors.computeIfAbsent(sequenceId, x -> new CopyOnWriteArraySet<>()).add(visitor);
  }

  /**
   * Fan-out a response to the relevant listener and use the executor to execute the callback. The gRPC network thread
   * is likely the party that's invoking this method so the executor is used to marshall the callback onto the proper
   * thread-pool for all "fdb" callbacks.
   * <p>
   * Most callbacks are guaranteed to be invoked once.
   *
   * @param resp Response to distribute.
   */
  void accept(StreamingDatabaseResponse resp) {
    if (resp.hasGetValue()) {
      ofNullable(sequenceResponseVisitors.remove(resp.getGetValue().getSequenceId())).orElse(Set.of()).
          forEach(x -> executor.execute(() -> x.handleGetValue(resp.getGetValue())));
    } else if (resp.hasGetKey()) {
      ofNullable(sequenceResponseVisitors.remove(resp.getGetKey().getSequenceId())).orElse(Set.of()).
          forEach(x -> executor.execute(() -> x.handleGetKey(resp.getGetKey())));
    } else if (resp.hasGetRange()) {
      // getRange can be called multiple times until done.
      if (resp.getGetRange().getDone()) {
        ofNullable(sequenceResponseVisitors.remove(resp.getGetRange().getSequenceId())).orElse(Set.of()).
            forEach(x -> executor.execute(() -> x.handleGetRange(resp.getGetRange())));
      } else {
        sequenceResponseVisitors.getOrDefault(resp.getGetRange().getSequenceId(), Set.of()).
            forEach(x -> executor.execute(() -> x.handleGetRange(resp.getGetRange())));
      }
    } else if (resp.hasOperationFailure()) {
      ofNullable(sequenceResponseVisitors.remove(resp.getOperationFailure().getSequenceId())).orElse(Set.of()).
          forEach(x -> executor.execute(() -> x.handleOperationFailure(resp.getOperationFailure())));
    } else if (resp.hasGetVersionstamp()) {
      ofNullable(sequenceResponseVisitors.remove(resp.getGetVersionstamp().getSequenceId())).orElse(Set.of()).
          forEach(x -> executor.execute(() -> x.handleGetVersionstamp(resp.getGetVersionstamp())));
    } else if (resp.hasGetReadVersion()) {
      ofNullable(sequenceResponseVisitors.remove(resp.getGetReadVersion().getSequenceId())).orElse(Set.of()).
          forEach(x -> executor.execute(() -> x.handleGetReadVersion(resp.getGetReadVersion())));
    } else if (resp.hasWatchKey()) {
      ofNullable(sequenceResponseVisitors.remove(resp.getWatchKey().getSequenceId())).orElse(Set.of()).
          forEach(x -> executor.execute(() -> x.handleGetWatchKey(resp.getWatchKey())));
    } else if (resp.hasGetApproximateSize()) {
      ofNullable(sequenceResponseVisitors.remove(resp.getGetApproximateSize().getSequenceId())).orElse(Set.of()).
          forEach(x -> executor.execute(() -> x.handleGetApproximateSize(resp.getGetApproximateSize())));
    } else if (resp.hasGetEstimatedRangeSize()) {
      ofNullable(sequenceResponseVisitors.remove(resp.getGetEstimatedRangeSize().getSequenceId())).orElse(Set.of()).
          forEach(x -> executor.execute(() -> x.handleGetEstimatedRangeSize(resp.getGetEstimatedRangeSize())));
    } else if (resp.hasGetBoundaryKeys()) {
      // getBoundaryKeys can be called multiple times until done.
      if (resp.getGetBoundaryKeys().getDone()) {
        ofNullable(sequenceResponseVisitors.remove(resp.getGetBoundaryKeys().getSequenceId())).orElse(Set.of()).
            forEach(x -> executor.execute(() -> x.handleGetBoundaryKeys(resp.getGetBoundaryKeys())));
      } else {
        sequenceResponseVisitors.getOrDefault(resp.getGetBoundaryKeys().getSequenceId(), Set.of()).
            forEach(x -> executor.execute(() -> x.handleGetBoundaryKeys(resp.getGetBoundaryKeys())));
      }
    } else if (resp.hasGetAddressesForKey()) {
      ofNullable(sequenceResponseVisitors.remove(resp.getGetAddressesForKey().getSequenceId())).orElse(Set.of()).
          forEach(x -> executor.execute(() -> x.handleGetAddressesForKey(resp.getGetAddressesForKey())));
    } else {
      throw new IllegalArgumentException("Unsupported response: " + resp);
    }
  }
}
