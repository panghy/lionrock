package io.github.panghy.lionrock.client.foundationdb.impl;

import io.github.panghy.lionrock.proto.*;

/**
 * Visitor interface for when a {@link io.github.panghy.lionrock.proto.StreamingDatabaseResponse}.
 *
 * @author Clement Pang
 */
public interface StreamingDatabaseResponseVisitor {

  void handleGetValue(GetValueResponse resp);

  void handleGetKey(GetKeyResponse resp);

  void handleGetRange(GetRangeResponse resp);

  void handleOperationFailure(OperationFailureResponse resp);

  void handleGetReadVersion(GetReadVersionResponse resp);

  void handleWatchKey(WatchKeyResponse resp);

  void handleGetApproximateSize(GetApproximateSizeResponse resp);

  void handleGetEstimatedRangeSize(GetEstimatedRangeSizeResponse resp);

  void handleGetBoundaryKeys(GetBoundaryKeysResponse resp);

  void handleGetAddressesForKey(GetAddressesForKeyResponse resp);
}
