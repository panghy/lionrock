package io.github.panghy.lionrock.client.foundationdb.impl;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import io.github.panghy.lionrock.client.foundationdb.RemoteFoundationDBDatabaseFactory;
import io.github.panghy.lionrock.tests.AbstractGrpcTest;
import org.junit.jupiter.api.BeforeEach;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AbstractFoundationDBClientTests extends AbstractGrpcTest {

  static final byte[] HELLO_B = "hello".getBytes(StandardCharsets.UTF_8);
  static final byte[] WORLD_B = "world".getBytes(StandardCharsets.UTF_8);
  static final CompletableFuture<?> DONE = CompletableFuture.completedFuture(null);
  static Database db;

  String getValue(Database stub, byte[] key) {
    byte[] run = stub.run(tx -> tx.get(key).join());
    byte[] run2 = stub.run(tx -> tx.snapshot().get(key).join());
    assertArrayEquals(run, run2);
    if (run == null) return null;
    return new String(run, StandardCharsets.UTF_8);
  }

  List<KeyValue> getRange(Database stub,
                          KeySelector start, KeySelector end, int limit, boolean reverse) {
    List<KeyValue> run = stub.run(tx -> tx.getRange(start, end, limit, reverse).asList().join());
    List<KeyValue> run2 = stub.run(tx -> tx.snapshot().getRange(start, end, limit, reverse).asList().join());
    assertEquals(run, run2);
    return run;
  }

  long clearRangeAndCommit(Database db, byte[] start, byte[] end) {
    Transaction tx = db.createTransaction();
    tx.clear(start, end);
    tx.commit().join();
    return tx.getCommittedVersion();
  }

  long setKeyAndCommit(Database db, byte[] key, byte[] value) {
    Transaction tx = db.createTransaction();
    tx.set(key, value);
    tx.commit().join();
    return tx.getCommittedVersion();
  }

  @BeforeEach
  public void setupChannel() {
    db = RemoteFoundationDBDatabaseFactory.open("fdb", "integration test", stub);
  }
}
