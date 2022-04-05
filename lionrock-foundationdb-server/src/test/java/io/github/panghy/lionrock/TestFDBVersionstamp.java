package io.github.panghy.lionrock;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestFDBVersionstamp {
  /**
   * Check that actual FDB client also hangs when using runAsync with getVersionstamp.
   */
  @Test
  public void testActualFDBGetVersionstamp() {
    Database fdb = FDB.selectAPIVersion(630).open();
    CompletableFuture<byte[]> cf = fdb.runAsync(Transaction::getVersionstamp);
    assertFalse(cf.isDone());
  }
}
