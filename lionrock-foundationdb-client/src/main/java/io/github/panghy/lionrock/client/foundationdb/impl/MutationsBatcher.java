package io.github.panghy.lionrock.client.foundationdb.impl;

import io.github.panghy.lionrock.proto.*;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Batcher of mutations that flushes regularly based on a threshold.
 */
public class MutationsBatcher {

  private final StreamObserver<StreamingDatabaseRequest> requestSink;
  private final BatchedMutationsRequest.Builder batchedMutationsRequest = BatchedMutationsRequest.newBuilder();
  private final AtomicLong size = new AtomicLong();
  private final long threshold;

  public MutationsBatcher(StreamObserver<StreamingDatabaseRequest> requestSink, long sizeThreshold) {
    this.requestSink = requestSink;
    this.threshold = sizeThreshold;
  }

  public synchronized void addClearKey(ClearKeyRequest req) {
    batchedMutationsRequest.addMutations(
        BatchedMutations.newBuilder().setClearKey(req).build());
    size.getAndAdd(req.getSerializedSize());
    flush(false);
  }

  public synchronized void addClearRange(ClearKeyRangeRequest req) {
    batchedMutationsRequest.addMutations(
        BatchedMutations.newBuilder().setClearRange(req).build());
    size.getAndAdd(req.getSerializedSize());
    flush(false);
  }

  public synchronized void addConflictKey(AddConflictKeyRequest req) {
    batchedMutationsRequest.addMutations(
        BatchedMutations.newBuilder().setAddConflictKey(req).build());
    size.getAndAdd(req.getSerializedSize());
    flush(false);
  }

  public synchronized void addConflictRange(AddConflictRangeRequest req) {
    batchedMutationsRequest.addMutations(
        BatchedMutations.newBuilder().setAddConflictRange(req).build());
    size.getAndAdd(req.getSerializedSize());
    flush(false);
  }

  public synchronized void addSetValue(SetValueRequest req) {
    batchedMutationsRequest.addMutations(
        BatchedMutations.newBuilder().setSetValue(req).build());
    size.getAndAdd(req.getSerializedSize());
    flush(false);
  }

  public synchronized void addMutateValue(MutateValueRequest req) {
    batchedMutationsRequest.addMutations(
        BatchedMutations.newBuilder().setMutateValue(req).build());
    size.getAndAdd(req.getSerializedSize());
    flush(false);
  }

  /**
   * Flush batched mutations.
   *
   * @param always whether to ignore {@link #threshold} and flush everything.
   */
  public synchronized void flush(boolean always) {
    if (always || size.get() > threshold) {
      synchronized (requestSink) {
        requestSink.onNext(StreamingDatabaseRequest.newBuilder().
            setBatchedMutations(batchedMutationsRequest).build());
      }
      batchedMutationsRequest.clear();
      size.set(0);
    }
  }
}
