package io.github.panghy.lionrock.client.foundationdb.impl;

/**
 * Since we do not want to call fdb_error_predicate in our library, we need to keep this list up-to-date with:
 * https://github.com/apple/foundationdb/blob/master/bindings/c/fdb_c.cpp#L65
 * <p>
 * Also see https://github.com/apple/foundationdb/blob/master/flow/error_definitions.h
 *
 * @author Clement Pang
 */
public abstract class FDBErrorCodes {

  public static final int error_code_commit_unknown_result = 1021;
  public static final int error_code_cluster_version_changed = 1039;
  public static final int error_code_not_committed = 1020;
  public static final int error_code_transaction_too_old = 1007;
  public static final int error_code_future_version = 1009;
  public static final int error_code_database_locked = 1038;
  public static final int error_code_proxy_memory_limit_exceeded = 1042;
  public static final int error_code_batch_transaction_throttled = 1051;
  public static final int error_code_process_behind = 1037;
  public static final int error_code_tag_throttled = 1213;

  public static boolean isRetryable(int code) {
    return maybeCommitted(code) || retryableNotCommited(code);
  }

  public static boolean maybeCommitted(int code) {
    return code == error_code_commit_unknown_result || code == error_code_cluster_version_changed;
  }

  public static boolean retryableNotCommited(int code) {
    return code == error_code_not_committed || code == error_code_transaction_too_old ||
        code == error_code_future_version || code == error_code_database_locked ||
        code == error_code_proxy_memory_limit_exceeded || code == error_code_batch_transaction_throttled ||
        code == error_code_process_behind || code == error_code_tag_throttled;
  }
}
