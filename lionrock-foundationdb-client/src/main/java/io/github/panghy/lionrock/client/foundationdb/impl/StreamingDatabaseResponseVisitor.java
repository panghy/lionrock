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

  void handleGetVersionstamp(GetVersionstampResponse resp);

  void handleGetReadVersion(GetReadVersionResponse resp);

  void handleGetWatchKey(WatchKeyResponse resp);

  void handleGetApproximateSize(GetApproximateSizeResponse resp);

  void handleGetEstimatedRangeSize(GetEstimatedRangeSizeResponse resp);
}
