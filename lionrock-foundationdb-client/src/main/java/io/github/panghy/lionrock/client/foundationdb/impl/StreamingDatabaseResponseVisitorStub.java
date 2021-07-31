package io.github.panghy.lionrock.client.foundationdb.impl;

import io.github.panghy.lionrock.proto.*;

/**
 * Stub implementation for {@link StreamingDatabaseResponseVisitor}.
 *
 * @author Clement Pang
 */
public interface StreamingDatabaseResponseVisitorStub extends StreamingDatabaseResponseVisitor {

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

  default void handleWatchKey(WatchKeyResponse resp) {
  }

  default void handleGetApproximateSize(GetApproximateSizeResponse resp) {
  }

  default void handleGetEstimatedRangeSize(GetEstimatedRangeSizeResponse resp) {
  }

  default void handleGetBoundaryKeys(GetBoundaryKeysResponse resp) {
  }

  default void handleGetAddressesForKey(GetAddressesForKeyResponse resp) {
  }
}
