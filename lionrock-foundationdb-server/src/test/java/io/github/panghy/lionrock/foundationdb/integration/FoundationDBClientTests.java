package io.github.panghy.lionrock.foundationdb.integration;

import com.apple.foundationdb.*;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import io.github.panghy.lionrock.client.foundationdb.RemoteFoundationDBDatabaseFactory;
import io.github.panghy.lionrock.foundationdb.AbstractGrpcTest;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class FoundationDBClientTests extends AbstractGrpcTest {

  private static final byte[] HELLO_B = "hello".getBytes(StandardCharsets.UTF_8);
  private static final byte[] WORLD_B = "world".getBytes(StandardCharsets.UTF_8);
  private static final CompletableFuture<?> DONE = CompletableFuture.completedFuture(null);

  @Test
  public void testGetReadVersion() {
    Database db = getDb();
    assertTrue(db.run(tx -> tx.getReadVersion().join()) > 0);
    assertTrue(db.runAsync(ReadTransaction::getReadVersion).join() > 0);
  }

  @Test
  public void testSetAndRead() {
    Database db = getDb();
    assertArrayEquals(
        WORLD_B, db.runAsync(tx -> {
          tx.set(HELLO_B, WORLD_B);
          return tx.get(HELLO_B);
        }).join());
  }

  @Test
  public void testSetAndRead_multiple() {
    Database db = getDb();
    db.runAsync(tx -> {
      tx.clear(HELLO_B);
      return CompletableFuture.completedFuture(null);
    }).join();
    assertNull(db.runAsync(tx -> tx.get(HELLO_B)).join());
    db.runAsync(tx -> {
      tx.set(HELLO_B, WORLD_B);
      return DONE;
    }).join();
    assertArrayEquals(
        WORLD_B, db.runAsync(tx -> tx.get(HELLO_B)).join());
    db.runAsync(tx -> {
      tx.clear(HELLO_B);
      return DONE;
    }).join();
    assertNull(db.runAsync(tx -> tx.get(HELLO_B)).join());
  }

  @Test
  void testStartTransaction_withInvalidDatabaseName() {
    Database database = RemoteFoundationDBDatabaseFactory.openPlainText("localhost", getPort(),
        "INVALID", "integration test");
    try {
      database.runAsync(ReadTransaction::getReadVersion).join();
      fail();
    } catch (CompletionException expected) {
    }
  }

  @Test
  void testClearRange() {
    Database stub = getDb();
    setupRangeTest(stub);

    assertEquals("world", getValue(stub, "hello".getBytes(StandardCharsets.UTF_8)));
    assertEquals("world", getValue(stub, "hello2".getBytes(StandardCharsets.UTF_8)));
    assertEquals("world", getValue(stub, "hello3".getBytes(StandardCharsets.UTF_8)));


    // end is exclusive. (should not delete hello).
    clearRangeAndCommit(stub, "hello".getBytes(StandardCharsets.UTF_8),
        "hello3".getBytes(StandardCharsets.UTF_8));

    assertNull(getValue(stub, "hello".getBytes(StandardCharsets.UTF_8)));
    assertNull(getValue(stub, "hello1".getBytes(StandardCharsets.UTF_8)));
    assertNull(getValue(stub, "hello2".getBytes(StandardCharsets.UTF_8)));
    assertEquals("world", getValue(stub, "hello3".getBytes(StandardCharsets.UTF_8)));
  }

  void setupRangeTest(Database stub) {
    clearRangeAndCommit(stub, "hello".getBytes(StandardCharsets.UTF_8),
        "hello4".getBytes(StandardCharsets.UTF_8));

    setKeyAndCommit(stub, "hello".getBytes(StandardCharsets.UTF_8), WORLD_B);
    setKeyAndCommit(stub, "hello1".getBytes(StandardCharsets.UTF_8), WORLD_B);
    setKeyAndCommit(stub, "hello2".getBytes(StandardCharsets.UTF_8), WORLD_B);
    setKeyAndCommit(stub, "hello3".getBytes(StandardCharsets.UTF_8), WORLD_B);
  }

  @Test
  public void testGetRange_explicitStartEnd() {
    Database stub = getDb();
    setupRangeTest(stub);

    List<KeyValue> results = getRange(stub,
        new KeySelector("hello".getBytes(StandardCharsets.UTF_8), false, 1),
        new KeySelector("hello3".getBytes(StandardCharsets.UTF_8), false, 1), 0, false);
    assertEquals(3, results.size());
    assertTrue(results.contains(new KeyValue(HELLO_B, WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello1".getBytes(StandardCharsets.UTF_8), WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello2".getBytes(StandardCharsets.UTF_8), WORLD_B)));
  }

  @Test
  public void testGetRange_keyselectorStart_firstGreaterThanOrEqual() {
    Database stub = getDb();
    setupRangeTest(stub);

    // firstGreaterOrEqual to Hello
    List<KeyValue> results = getRange(stub,
        KeySelector.firstGreaterOrEqual("hello".getBytes(StandardCharsets.UTF_8)),
        new KeySelector("hello3".getBytes(StandardCharsets.UTF_8), false, 1), 0, false);
    assertEquals(3, results.size());
    assertTrue(results.contains(new KeyValue(HELLO_B, WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello1".getBytes(StandardCharsets.UTF_8), WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello2".getBytes(StandardCharsets.UTF_8), WORLD_B)));
  }

  @Test
  public void testGetRange_keyselectorStart_lastLessThan() {
    Database stub = getDb();
    setupRangeTest(stub);

    // lastLessThan to Hello
    List<KeyValue> results = getRange(stub,
        KeySelector.lastLessThan("hello0".getBytes(StandardCharsets.UTF_8)),
        new KeySelector("hello3".getBytes(StandardCharsets.UTF_8), false, 1), 0, false);
    assertEquals(3, results.size());
    assertTrue(results.contains(new KeyValue(HELLO_B, WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello1".getBytes(StandardCharsets.UTF_8), WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello2".getBytes(StandardCharsets.UTF_8), WORLD_B)));
  }

  @Test
  public void testGetRange_keyselectorStart_lastLessThanOrEqual() {
    Database stub = getDb();
    setupRangeTest(stub);

    // lastLessThanOrEqual "hello"
    List<KeyValue> results = getRange(stub,
        KeySelector.lastLessOrEqual("hello".getBytes(StandardCharsets.UTF_8)),
        new KeySelector("hello3".getBytes(StandardCharsets.UTF_8), false, 1), 0, false);
    assertEquals(3, results.size());
    assertTrue(results.contains(new KeyValue(HELLO_B, WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello1".getBytes(StandardCharsets.UTF_8), WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello2".getBytes(StandardCharsets.UTF_8), WORLD_B)));
  }

  @Test
  public void testGetRange_keyselectorEnd_firstGreaterThan() {
    Database stub = getDb();
    setupRangeTest(stub);

    // firstAfterOrEqual to Hello
    List<KeyValue> results = getRange(stub,
        new KeySelector("hello".getBytes(StandardCharsets.UTF_8), false, 1),
        KeySelector.firstGreaterThan("hello2".getBytes(StandardCharsets.UTF_8)), 0, false);
    assertEquals(3, results.size());
    assertTrue(results.contains(new KeyValue(HELLO_B, WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello1".getBytes(StandardCharsets.UTF_8), WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello2".getBytes(StandardCharsets.UTF_8), WORLD_B)));
  }

  @Test
  public void testGetRange_keyselectorEnd_firstGreaterThanOrEqual() {
    Database stub = getDb();
    setupRangeTest(stub);

    // firstAfterOrEqual to Hello
    List<KeyValue> results = getRange(stub,
        new KeySelector("hello".getBytes(StandardCharsets.UTF_8), false, 1),
        KeySelector.firstGreaterOrEqual("hello3".getBytes(StandardCharsets.UTF_8)), 0, false);
    assertEquals(3, results.size());
    assertTrue(results.contains(new KeyValue(HELLO_B, WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello1".getBytes(StandardCharsets.UTF_8), WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello2".getBytes(StandardCharsets.UTF_8), WORLD_B)));
  }

  @Test
  public void testGetRange_keyselectorEnd_lastLessThanOrEqual() {
    Database stub = getDb();
    setupRangeTest(stub);

    // lastLessThanOrEqual "hello"
    List<KeyValue> results = getRange(stub,
        new KeySelector("hello".getBytes(StandardCharsets.UTF_8), false, 1),
        KeySelector.lastLessOrEqual("hello3".getBytes(StandardCharsets.UTF_8)), 0, false);
    assertEquals(3, results.size());
    assertTrue(results.contains(new KeyValue(HELLO_B, WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello1".getBytes(StandardCharsets.UTF_8), WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello2".getBytes(StandardCharsets.UTF_8), WORLD_B)));
  }

  @Test
  public void testGetRange_keyselectorEnd_lastLessThan() {
    Database stub = getDb();
    setupRangeTest(stub);

    // lastLessThan hello3 which is hello2.
    List<KeyValue> results = getRange(stub,
        new KeySelector("hello".getBytes(StandardCharsets.UTF_8), false, 1),
        KeySelector.lastLessThan("hello3".getBytes(StandardCharsets.UTF_8)), 0, false);
    assertEquals(2, results.size());
    assertTrue(results.contains(new KeyValue(HELLO_B, WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello1".getBytes(StandardCharsets.UTF_8), WORLD_B)));
  }

  @Test
  public void testEmptyRangeRead() {
    Database stub = getDb();
    clearRangeAndCommit(stub, "hello".getBytes(StandardCharsets.UTF_8),
        "hello4".getBytes(StandardCharsets.UTF_8));

    List<KeyValue> range = getRange(stub, KeySelector.firstGreaterOrEqual(HELLO_B),
        KeySelector.firstGreaterOrEqual("hello4".getBytes(StandardCharsets.UTF_8)), 0, false);
    assertTrue(range.isEmpty());
  }

  @Test
  void testSetValue_andGetVersionstamp() {
    Database stub = getDb();
    byte[] join = stub.run(tx -> {
      tx.mutate(MutationType.SET_VERSIONSTAMPED_KEY,
          Tuple.from("hello", Versionstamp.incomplete()).packWithVersionstamp(),
          WORLD_B);
      return tx.getVersionstamp();
    }).join();
    assertNotNull(join);
    byte[] helloB = Tuple.from("hello", Versionstamp.complete(join, 0)).pack();
    assertArrayEquals(WORLD_B, stub.run(tx -> tx.get(helloB).join()));
  }

  @Test
  void testSetValue_andGetApproximateSize() {
    Database stub = getDb();
    long join = stub.run(tx -> {
      tx.get(HELLO_B).join();
      tx.set(HELLO_B, WORLD_B);
      return tx.getApproximateSize().join();
    });
    assertEquals(108, join);
  }

  @Test
  void testInvalidGetVersionstamp() throws InterruptedException, TimeoutException {
    Database stub = getDb();
    CompletableFuture<byte[]> cf = stub.run(Transaction::getVersionstamp);
    assertNotNull(cf);
    try {
      cf.get(5, TimeUnit.SECONDS);
      fail();
    } catch (ExecutionException expected) {
    }
  }

  @Test
  void testWatch() throws ExecutionException, InterruptedException, TimeoutException {
    Database stub = getDb();
    CompletableFuture<Void> cf = stub.run(tx -> {
      tx.get(HELLO_B).join();
      tx.clear(HELLO_B);
      return tx.watch(HELLO_B);
    });
    assertFalse(cf.isDone());
    setKeyAndCommit(stub, HELLO_B, WORLD_B);
    cf.get(5, TimeUnit.SECONDS);
  }

  @Test
  void testRetry() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(2);
    Database stub = getDb();
    AtomicInteger loop1 = new AtomicInteger();
    AtomicInteger loop2 = new AtomicInteger();
    Thread t1 = new Thread(() -> stub.run(tx -> {
      loop1.incrementAndGet();
      tx.get(HELLO_B).join();
      tx.set(HELLO_B, WORLD_B);
      latch.countDown();
      try {
        latch.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return null;
    }));
    Thread t2 = new Thread(() -> stub.run(tx -> {
      loop2.incrementAndGet();
      tx.get(HELLO_B).join();
      tx.set(HELLO_B, WORLD_B);
      latch.countDown();
      try {
        latch.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return null;
    }));
    t1.start();
    t2.start();
    t1.join();
    t2.join();
    // one of the loops should retry.
    assertEquals(2, Math.max(loop1.get(), loop2.get()));
  }

  String getValue(Database stub, byte[] key) {
    byte[] run = stub.run(tx -> tx.get(key).join());
    if (run == null) return null;
    return new String(run, StandardCharsets.UTF_8);
  }

  List<KeyValue> getRange(Database stub,
                          KeySelector start, KeySelector end, int limit, boolean reverse) {
    return stub.run(tx -> tx.getRange(start, end, limit, reverse).asList().join());
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

  private Database getDb() {
    return RemoteFoundationDBDatabaseFactory.openPlainText("localhost", getPort(),
        "fdb", "integration test");
  }
}
