package io.github.panghy.lionrock.client.foundationdb.impl;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FDBErrorCodesTest {

  @Test
  void isRetryable() {
    assertTrue(FDBErrorCodes.isRetryable(FDBErrorCodes.error_code_not_committed));
    assertTrue(FDBErrorCodes.isRetryable(FDBErrorCodes.error_code_transaction_too_old));
    assertTrue(FDBErrorCodes.isRetryable(FDBErrorCodes.error_code_future_version));
    assertTrue(FDBErrorCodes.isRetryable(FDBErrorCodes.error_code_database_locked));
    assertTrue(FDBErrorCodes.isRetryable(FDBErrorCodes.error_code_proxy_memory_limit_exceeded));
    assertTrue(FDBErrorCodes.isRetryable(FDBErrorCodes.error_code_batch_transaction_throttled));
    assertTrue(FDBErrorCodes.isRetryable(FDBErrorCodes.error_code_process_behind));
    assertTrue(FDBErrorCodes.isRetryable(FDBErrorCodes.error_code_tag_throttled));
    assertTrue(FDBErrorCodes.isRetryable(FDBErrorCodes.error_code_commit_unknown_result));
    assertTrue(FDBErrorCodes.isRetryable(FDBErrorCodes.error_code_cluster_version_changed));

    assertFalse(FDBErrorCodes.isRetryable(1));
  }

  @Test
  void maybeCommitted() {
    assertTrue(FDBErrorCodes.maybeCommitted(FDBErrorCodes.error_code_commit_unknown_result));
    assertTrue(FDBErrorCodes.maybeCommitted(FDBErrorCodes.error_code_cluster_version_changed));

    assertFalse(FDBErrorCodes.maybeCommitted(FDBErrorCodes.error_code_not_committed));
    assertFalse(FDBErrorCodes.maybeCommitted(FDBErrorCodes.error_code_transaction_too_old));
    assertFalse(FDBErrorCodes.maybeCommitted(FDBErrorCodes.error_code_future_version));
    assertFalse(FDBErrorCodes.maybeCommitted(FDBErrorCodes.error_code_database_locked));
    assertFalse(FDBErrorCodes.maybeCommitted(FDBErrorCodes.error_code_proxy_memory_limit_exceeded));
    assertFalse(FDBErrorCodes.maybeCommitted(FDBErrorCodes.error_code_batch_transaction_throttled));
    assertFalse(FDBErrorCodes.maybeCommitted(FDBErrorCodes.error_code_process_behind));
    assertFalse(FDBErrorCodes.maybeCommitted(FDBErrorCodes.error_code_tag_throttled));

    assertFalse(FDBErrorCodes.maybeCommitted(1));
  }

  @Test
  void retryableNotCommited() {
    assertTrue(FDBErrorCodes.retryableNotCommited(FDBErrorCodes.error_code_not_committed));
    assertTrue(FDBErrorCodes.retryableNotCommited(FDBErrorCodes.error_code_transaction_too_old));
    assertTrue(FDBErrorCodes.retryableNotCommited(FDBErrorCodes.error_code_future_version));
    assertTrue(FDBErrorCodes.retryableNotCommited(FDBErrorCodes.error_code_database_locked));
    assertTrue(FDBErrorCodes.retryableNotCommited(FDBErrorCodes.error_code_proxy_memory_limit_exceeded));
    assertTrue(FDBErrorCodes.retryableNotCommited(FDBErrorCodes.error_code_batch_transaction_throttled));
    assertTrue(FDBErrorCodes.retryableNotCommited(FDBErrorCodes.error_code_process_behind));
    assertTrue(FDBErrorCodes.retryableNotCommited(FDBErrorCodes.error_code_tag_throttled));

    assertFalse(FDBErrorCodes.retryableNotCommited(FDBErrorCodes.error_code_commit_unknown_result));
    assertFalse(FDBErrorCodes.retryableNotCommited(FDBErrorCodes.error_code_cluster_version_changed));

    assertFalse(FDBErrorCodes.retryableNotCommited(1));
  }
}