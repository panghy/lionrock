package io.github.panghy.lionrock.client.foundationdb.impl;

import io.github.panghy.lionrock.proto.*;

/**
 * Visitor interface for when a {@link io.github.panghy.lionrock.proto.StreamingDatabaseResponse}.
 *
 * @author Clement Pang
 */
public interface StreamingDatabaseResponseVisitor {

  default void handleCommitTransaction(CommitTransactionResponse resp) {
  }

  default void handleGetValue(GetValueResponse resp) {
  }

  default void handleGetKey(GetKeyResponse resp) {
  }

  default void handleGetRange(GetRangeResponse resp) {
  }

  default void handleOperationFailure(OperationFailureResponse resp) {
  }

  default void handleGetVersionstamp(GetVersionstampResponse resp) {
  }

  default void handleGetReadVersion(GetReadVersionResponse resp) {
  }

  default void handleGetWatchKey(WatchKeyResponse resp) {
  }

  default void handleGetApproximateSize(GetApproximateSizeResponse resp) {
  }
}
