package io.github.panghy.lionrock.client.foundationdb.impl;

import io.github.panghy.lionrock.proto.StreamingDatabaseResponse;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Takes a {@link io.github.panghy.lionrock.proto.StreamingDatabaseResponse} and distributes the response to a number
 * of clients.
 *
 * @author Clement Pang
 */
public class SequenceResponseDemuxer {

  private final ConcurrentMap<Long, StreamingDatabaseResponseVisitor> sequenceResponseVisitors =
      new ConcurrentHashMap<>();
  /**
   * Allow us to debug sequence ids easier.
   */
  private final AtomicLong sequenceId = new AtomicLong((long) (Math.random() * Long.MAX_VALUE));
  private final Set<Long> knownSequenceIds = new HashSet<>();

  long addHandler(StreamingDatabaseResponseVisitor visitor) {
    long toReturn = sequenceId.incrementAndGet();
    knownSequenceIds.add(toReturn);
    if (sequenceResponseVisitors.putIfAbsent(toReturn, visitor) != null) {
      throw new IllegalArgumentException("sequenceId: " + sequenceId + " is already registered");
    }
    return toReturn;
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
      removeVistorOrThrow(resp.getGetValue().getSequenceId()).handleGetValue(resp.getGetValue());
    } else if (resp.hasGetKey()) {
      removeVistorOrThrow(resp.getGetKey().getSequenceId()).handleGetKey(resp.getGetKey());
    } else if (resp.hasGetRange()) {
      // getRange can be called multiple times until done.
      if (resp.getGetRange().getDone()) {
        removeVistorOrThrow(resp.getGetRange().getSequenceId()).handleGetRange(resp.getGetRange());
      } else {
        getVistorOrThrow(resp.getGetRange().getSequenceId()).handleGetRange(resp.getGetRange());
      }
    } else if (resp.hasOperationFailure()) {
      removeVistorOrThrow(resp.getOperationFailure().getSequenceId()).
          handleOperationFailure(resp.getOperationFailure());
    } else if (resp.hasGetReadVersion()) {
      removeVistorOrThrow(resp.getGetReadVersion().getSequenceId()).
          handleGetReadVersion(resp.getGetReadVersion());
    } else if (resp.hasWatchKey()) {
      removeVistorOrThrow(resp.getWatchKey().getSequenceId()).handleWatchKey(resp.getWatchKey());
    } else if (resp.hasGetApproximateSize()) {
      removeVistorOrThrow(resp.getGetApproximateSize().getSequenceId()).
          handleGetApproximateSize(resp.getGetApproximateSize());
    } else if (resp.hasGetEstimatedRangeSize()) {
      removeVistorOrThrow(resp.getGetEstimatedRangeSize().getSequenceId()).
          handleGetEstimatedRangeSize(resp.getGetEstimatedRangeSize());
    } else if (resp.hasGetBoundaryKeys()) {
      // getBoundaryKeys can be called multiple times until done.
      if (resp.getGetBoundaryKeys().getDone()) {
        removeVistorOrThrow(resp.getGetBoundaryKeys().getSequenceId()).
            handleGetBoundaryKeys(resp.getGetBoundaryKeys());
      } else {
        getVistorOrThrow(resp.getGetBoundaryKeys().getSequenceId()).
            handleGetBoundaryKeys(resp.getGetBoundaryKeys());
      }
    } else if (resp.hasGetAddressesForKey()) {
      removeVistorOrThrow(resp.getGetAddressesForKey().getSequenceId()).
          handleGetAddressesForKey(resp.getGetAddressesForKey());
    } else {
      throw new IllegalArgumentException("Unsupported response: " + resp);
    }
  }

  private StreamingDatabaseResponseVisitor getVistorOrThrow(long sequenceId) {
    if (!sequenceResponseVisitors.containsKey(sequenceId)) {
      if (knownSequenceIds.contains(sequenceId)) {
        throw new IllegalArgumentException("sequenceId: " + sequenceId + " already removed by prior callback");
      }
      throw new IllegalArgumentException("Unknown sequenceId: " + sequenceId);
    }
    return sequenceResponseVisitors.get(sequenceId);
  }

  private StreamingDatabaseResponseVisitor removeVistorOrThrow(long sequenceId) {
    if (!sequenceResponseVisitors.containsKey(sequenceId)) {
      if (knownSequenceIds.contains(sequenceId)) {
        throw new IllegalArgumentException("sequenceId: " + sequenceId + " already removed by prior callback");
      }
      throw new IllegalArgumentException("Unknown sequenceId: " + sequenceId);
    }
    return sequenceResponseVisitors.remove(sequenceId);
  }
}
