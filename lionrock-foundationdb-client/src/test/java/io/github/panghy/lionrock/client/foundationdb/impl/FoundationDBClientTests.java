package io.github.panghy.lionrock.client.foundationdb.impl;

import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import io.github.panghy.lionrock.client.foundationdb.RemoteFoundationDBDatabaseFactory;
import io.github.panghy.lionrock.client.foundationdb.RemoteLocalityUtil;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class FoundationDBClientTests extends AbstractFoundationDBClientTests {

  @Test
  public void testGetReadVersion() {
    assertTrue(db.run(tx -> tx.getReadVersion().join()) > 0);
    assertTrue(db.runAsync(ReadTransaction::getReadVersion).join() > 0);
  }

  @Test
  public void testSetAndRead() {
    assertArrayEquals(
        WORLD_B, db.runAsync(tx -> {
          tx.set(HELLO_B, WORLD_B);
          return tx.get(HELLO_B);
        }).join());
  }

  @Test
  public void testSetAndRead_multiple() {
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
    Database database = RemoteFoundationDBDatabaseFactory.open("INVALID", "integration test", channel);
    try {
      database.runAsync(ReadTransaction::getReadVersion).join();
      fail();
    } catch (CompletionException expected) {
    }
  }

  @Test
  void testClearRange() {
    setupRangeTest(db);

    assertEquals("world", getValue(db, "hello".getBytes(StandardCharsets.UTF_8)));
    assertEquals("world", getValue(db, "hello2".getBytes(StandardCharsets.UTF_8)));
    assertEquals("world", getValue(db, "hello3".getBytes(StandardCharsets.UTF_8)));


    // end is exclusive. (should not delete hello).
    clearRangeAndCommit(db, "hello".getBytes(StandardCharsets.UTF_8),
        "hello3".getBytes(StandardCharsets.UTF_8));

    assertNull(getValue(db, "hello".getBytes(StandardCharsets.UTF_8)));
    assertNull(getValue(db, "hello1".getBytes(StandardCharsets.UTF_8)));
    assertNull(getValue(db, "hello2".getBytes(StandardCharsets.UTF_8)));
    assertEquals("world", getValue(db, "hello3".getBytes(StandardCharsets.UTF_8)));
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
    setupRangeTest(db);

    List<KeyValue> results = getRange(db,
        new KeySelector("hello".getBytes(StandardCharsets.UTF_8), false, 1),
        new KeySelector("hello3".getBytes(StandardCharsets.UTF_8), false, 1), 0, false);
    assertEquals(3, results.size());
    assertTrue(results.contains(new KeyValue(HELLO_B, WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello1".getBytes(StandardCharsets.UTF_8), WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello2".getBytes(StandardCharsets.UTF_8), WORLD_B)));
  }

  @Test
  public void testGetRange_explicitStartEnd_limit1() {
    setupRangeTest(db);

    List<KeyValue> results = getRange(db,
        new KeySelector("hello".getBytes(StandardCharsets.UTF_8), false, 1),
        new KeySelector("hello3".getBytes(StandardCharsets.UTF_8), false, 1), 1, false);
    assertEquals(1, results.size());
    assertTrue(results.contains(new KeyValue(HELLO_B, WORLD_B)));
  }

  @Test
  public void testGetRange_explicitStartEnd_limit1_reverse() {
    setupRangeTest(db);

    List<KeyValue> results = getRange(db,
        new KeySelector("hello".getBytes(StandardCharsets.UTF_8), false, 1),
        new KeySelector("hello3".getBytes(StandardCharsets.UTF_8), false, 1), 1, true);
    assertEquals(1, results.size());
    assertTrue(results.contains(new KeyValue("hello2".getBytes(StandardCharsets.UTF_8), WORLD_B)));
  }

  @Test
  public void testGetRange_explicitStartEnd_wantAll() {
    setupRangeTest(db);

    List<KeyValue> results = db.run(tx -> tx.getRange(
            new KeySelector("hello".getBytes(StandardCharsets.UTF_8), false, 1),
            new KeySelector("hello3".getBytes(StandardCharsets.UTF_8), false, 1), 3, true, StreamingMode.WANT_ALL).
        asList().join());
    assertEquals(3, results.size());
    assertTrue(results.contains(new KeyValue(HELLO_B, WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello1".getBytes(StandardCharsets.UTF_8), WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello2".getBytes(StandardCharsets.UTF_8), WORLD_B)));
  }

  @Test
  public void testGetRange_explicitStartEnd_iterator() {
    setupRangeTest(db);

    List<KeyValue> results = db.run(tx -> tx.getRange(
            new KeySelector("hello".getBytes(StandardCharsets.UTF_8), false, 1),
            new KeySelector("hello3".getBytes(StandardCharsets.UTF_8), false, 1), 3, true, StreamingMode.ITERATOR).
        asList().join());
    assertEquals(3, results.size());
    assertTrue(results.contains(new KeyValue(HELLO_B, WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello1".getBytes(StandardCharsets.UTF_8), WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello2".getBytes(StandardCharsets.UTF_8), WORLD_B)));
  }

  @Test
  public void testGetRange_explicitStartEnd_exact() {
    setupRangeTest(db);

    List<KeyValue> results = db.run(tx -> tx.getRange(
            new KeySelector("hello".getBytes(StandardCharsets.UTF_8), false, 1),
            new KeySelector("hello3".getBytes(StandardCharsets.UTF_8), false, 1), 3, true, StreamingMode.EXACT).
        asList().join());
    assertEquals(3, results.size());
    assertTrue(results.contains(new KeyValue(HELLO_B, WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello1".getBytes(StandardCharsets.UTF_8), WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello2".getBytes(StandardCharsets.UTF_8), WORLD_B)));
  }

  @Test
  public void testGetRange_keyselectorStart_firstGreaterThanOrEqual() {
    setupRangeTest(db);

    // firstGreaterOrEqual to Hello
    List<KeyValue> results = getRange(db,
        KeySelector.firstGreaterOrEqual("hello".getBytes(StandardCharsets.UTF_8)),
        new KeySelector("hello3".getBytes(StandardCharsets.UTF_8), false, 1), 0, false);
    assertEquals(3, results.size());
    assertTrue(results.contains(new KeyValue(HELLO_B, WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello1".getBytes(StandardCharsets.UTF_8), WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello2".getBytes(StandardCharsets.UTF_8), WORLD_B)));
  }

  @Test
  public void testGetRange_keyselectorStart_lastLessThan() {
    setupRangeTest(db);

    // lastLessThan to Hello
    List<KeyValue> results = getRange(db,
        KeySelector.lastLessThan("hello0".getBytes(StandardCharsets.UTF_8)),
        new KeySelector("hello3".getBytes(StandardCharsets.UTF_8), false, 1), 0, false);
    assertEquals(3, results.size());
    assertTrue(results.contains(new KeyValue(HELLO_B, WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello1".getBytes(StandardCharsets.UTF_8), WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello2".getBytes(StandardCharsets.UTF_8), WORLD_B)));
  }

  @Test
  public void testGetRange_keyselectorStart_lastLessThanOrEqual() {
    setupRangeTest(db);

    // lastLessThanOrEqual "hello"
    List<KeyValue> results = getRange(db,
        KeySelector.lastLessOrEqual("hello".getBytes(StandardCharsets.UTF_8)),
        new KeySelector("hello3".getBytes(StandardCharsets.UTF_8), false, 1), 0, false);
    assertEquals(3, results.size());
    assertTrue(results.contains(new KeyValue(HELLO_B, WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello1".getBytes(StandardCharsets.UTF_8), WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello2".getBytes(StandardCharsets.UTF_8), WORLD_B)));
  }

  @Test
  public void testGetRange_keyselectorEnd_firstGreaterThan() {
    setupRangeTest(db);

    // firstAfterOrEqual to Hello
    List<KeyValue> results = getRange(db,
        new KeySelector("hello".getBytes(StandardCharsets.UTF_8), false, 1),
        KeySelector.firstGreaterThan("hello2".getBytes(StandardCharsets.UTF_8)), 0, false);
    assertEquals(3, results.size());
    assertTrue(results.contains(new KeyValue(HELLO_B, WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello1".getBytes(StandardCharsets.UTF_8), WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello2".getBytes(StandardCharsets.UTF_8), WORLD_B)));
  }

  @Test
  public void testGetRange_keyselectorEnd_firstGreaterThanOrEqual() {
    setupRangeTest(db);

    // firstAfterOrEqual to Hello
    List<KeyValue> results = getRange(db,
        new KeySelector("hello".getBytes(StandardCharsets.UTF_8), false, 1),
        KeySelector.firstGreaterOrEqual("hello3".getBytes(StandardCharsets.UTF_8)), 0, false);
    assertEquals(3, results.size());
    assertTrue(results.contains(new KeyValue(HELLO_B, WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello1".getBytes(StandardCharsets.UTF_8), WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello2".getBytes(StandardCharsets.UTF_8), WORLD_B)));
  }

  @Test
  public void testGetRange_keyselectorEnd_lastLessThanOrEqual() {
    setupRangeTest(db);

    // lastLessThanOrEqual "hello"
    List<KeyValue> results = getRange(db,
        new KeySelector("hello".getBytes(StandardCharsets.UTF_8), false, 1),
        KeySelector.lastLessOrEqual("hello3".getBytes(StandardCharsets.UTF_8)), 0, false);
    assertEquals(3, results.size());
    assertTrue(results.contains(new KeyValue(HELLO_B, WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello1".getBytes(StandardCharsets.UTF_8), WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello2".getBytes(StandardCharsets.UTF_8), WORLD_B)));
  }

  @Test
  public void testGetRange_keyselectorEnd_lastLessThan() {
    setupRangeTest(db);

    // lastLessThan hello3 which is hello2.
    List<KeyValue> results = getRange(db,
        new KeySelector("hello".getBytes(StandardCharsets.UTF_8), false, 1),
        KeySelector.lastLessThan("hello3".getBytes(StandardCharsets.UTF_8)), 0, false);
    assertEquals(2, results.size());
    assertTrue(results.contains(new KeyValue(HELLO_B, WORLD_B)));
    assertTrue(results.contains(new KeyValue("hello1".getBytes(StandardCharsets.UTF_8), WORLD_B)));
  }

  @Test
  public void testEmptyRangeRead() {
    clearRangeAndCommit(db, "hello".getBytes(StandardCharsets.UTF_8),
        "hello4".getBytes(StandardCharsets.UTF_8));

    List<KeyValue> range = getRange(db, KeySelector.firstGreaterOrEqual(HELLO_B),
        KeySelector.firstGreaterOrEqual("hello4".getBytes(StandardCharsets.UTF_8)), 0, false);
    assertTrue(range.isEmpty());
  }

  @Test
  void testSetValue_andGetVersionstamp() {
    byte[] join = db.run(tx -> {
      tx.mutate(MutationType.SET_VERSIONSTAMPED_KEY,
          Tuple.from("hello", Versionstamp.incomplete()).packWithVersionstamp(),
          WORLD_B);
      return tx.getVersionstamp();
    }).join();
    assertNotNull(join);
    byte[] helloB = Tuple.from("hello", Versionstamp.complete(join, 0)).pack();
    assertArrayEquals(WORLD_B, db.run(tx -> tx.get(helloB).join()));
  }

  @Test
  void testSetValue_andGetApproximateSize() {
    long join = db.run(tx -> {
      tx.get(HELLO_B).join();
      tx.set(HELLO_B, WORLD_B);
      return tx.getApproximateSize().join();
    });
    assertEquals(108, join);
  }

  @Test
  void testInvalidGetVersionstamp() throws InterruptedException, TimeoutException {
    CompletableFuture<byte[]> cf = db.run(Transaction::getVersionstamp);
    assertNotNull(cf);
    try {
      cf.get(5, TimeUnit.SECONDS);
      fail();
    } catch (ExecutionException expected) {
    }
  }

  @Test
  void testWatch() throws ExecutionException, InterruptedException, TimeoutException {
    CompletableFuture<Void> cf = db.run(tx -> {
      tx.get(HELLO_B).join();
      tx.clear(HELLO_B);
      return tx.watch(HELLO_B);
    });
    assertFalse(cf.isDone());
    setKeyAndCommit(db, HELLO_B, WORLD_B);
    cf.get(5, TimeUnit.SECONDS);
  }

  @Test
  void testRetry() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(2);
    AtomicInteger loop1 = new AtomicInteger();
    AtomicInteger loop2 = new AtomicInteger();
    Thread t1 = new Thread(() -> db.run(tx -> {
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
    Thread t2 = new Thread(() -> db.run(tx -> {
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

  /**
   * To setup this test, we run two transactions, one performing a blind write against a range that was read first by
   * another one. Since blind writes should always go through, we expect the read-then-write transaction to fail.
   */
  @Test
  public void testWriteConflict() throws InterruptedException {
    // clear hello -> hello4
    clearRangeAndCommit(db, "hello".getBytes(StandardCharsets.UTF_8),
        "hello4".getBytes(StandardCharsets.UTF_8));

    CountDownLatch txLatch = new CountDownLatch(1);
    CountDownLatch writeLatch = new CountDownLatch(1);
    AtomicBoolean called = new AtomicBoolean();
    // tx1.
    CompletableFuture<String> cf = db.runAsync(tx -> {
      if (called.getAndSet(true)) {
        return CompletableFuture.completedFuture("not written");
      }
      return tx.getRange(HELLO_B, "hello4".getBytes(StandardCharsets.UTF_8)).asList().
          thenApplyAsync(kvs -> {
            txLatch.countDown();
            assertTrue(kvs.isEmpty());
            try {
              writeLatch.await();
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            tx.set(HELLO_B, "bar".getBytes(StandardCharsets.UTF_8));
            return "written";
          });
    });
    // wait until tx1 has gotten a read version and is about to commit.
    txLatch.await();
    // tx2.
    db.run(tx -> {
      tx.set(HELLO_B, WORLD_B);
      return null;
    });
    writeLatch.countDown();
    assertEquals("not written", cf.join());
    assertEquals("world", getValue(db, "hello".getBytes(StandardCharsets.UTF_8)));
  }

  /**
   * To setup this test, we run two transactions, one performing a blind write against a range that was read by
   * another one (via conflict range read). Since blind writes should always go through, we expect the read-then-write
   * transaction to fail.
   */
  @Test
  public void testAddReadConflictRange() throws InterruptedException {
    // clear hello -> hello4
    clearRangeAndCommit(db, "hello".getBytes(StandardCharsets.UTF_8),
        "hello4".getBytes(StandardCharsets.UTF_8));

    CountDownLatch txLatch = new CountDownLatch(1);
    CountDownLatch writeLatch = new CountDownLatch(1);
    AtomicBoolean called = new AtomicBoolean();
    // tx1.
    CompletableFuture<String> cf = db.runAsync(tx -> {
      if (called.getAndSet(true)) {
        return CompletableFuture.completedFuture("not written");
      }
      return tx.getReadVersion().
          thenApplyAsync(v -> {
            tx.addReadConflictRange(HELLO_B, "hello4".getBytes(StandardCharsets.UTF_8));
            txLatch.countDown();
            try {
              writeLatch.await();
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            tx.set(HELLO_B, "bar".getBytes(StandardCharsets.UTF_8));
            return "written";
          });
    });
    // wait until tx1 has gotten a read version and is about to commit.
    txLatch.await();
    // tx2.
    db.run(tx -> {
      tx.set(HELLO_B, WORLD_B);
      return null;
    });
    writeLatch.countDown();
    assertEquals("not written", cf.join());
    assertEquals("world", getValue(db, "hello".getBytes(StandardCharsets.UTF_8)));
  }

  /**
   * To setup this test, we run two transactions, one performing a blind write against a range that was read by
   * another one (via conflict range read). Since blind writes should always go through, we expect the read-then-write
   * transaction to fail.
   */
  @Test
  public void testAddReadConflictKey() throws InterruptedException {
    // clear hello -> hello4
    clearRangeAndCommit(db, "hello".getBytes(StandardCharsets.UTF_8),
        "hello4".getBytes(StandardCharsets.UTF_8));

    CountDownLatch txLatch = new CountDownLatch(1);
    CountDownLatch writeLatch = new CountDownLatch(1);
    AtomicBoolean called = new AtomicBoolean();
    // tx1.
    CompletableFuture<String> cf = db.runAsync(tx -> {
      if (called.getAndSet(true)) {
        return CompletableFuture.completedFuture("not written");
      }
      return tx.getReadVersion().
          thenApplyAsync(v -> {
            tx.addReadConflictKey(HELLO_B);
            txLatch.countDown();
            try {
              writeLatch.await();
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            tx.set(HELLO_B, "bar".getBytes(StandardCharsets.UTF_8));
            return "written";
          });
    });
    // wait until tx1 has gotten a read version and is about to commit.
    txLatch.await();
    // tx2.
    db.run(tx -> {
      tx.set(HELLO_B, WORLD_B);
      return null;
    });
    writeLatch.countDown();
    assertEquals("not written", cf.join());
    assertEquals("world", getValue(db, "hello".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void testAddWriteConflictRange() throws InterruptedException {
    // clear hello -> hello4
    clearRangeAndCommit(db, "hello".getBytes(StandardCharsets.UTF_8),
        "hello4".getBytes(StandardCharsets.UTF_8));

    CountDownLatch txLatch = new CountDownLatch(1);
    CountDownLatch writeLatch = new CountDownLatch(1);
    AtomicBoolean called = new AtomicBoolean();
    // tx1.
    CompletableFuture<String> cf = db.runAsync(tx -> {
      if (called.getAndSet(true)) {
        return CompletableFuture.completedFuture("not written");
      }
      return tx.getRange(HELLO_B, "hello4".getBytes(StandardCharsets.UTF_8)).asList().
          thenApplyAsync(kvs -> {
            assertTrue(kvs.isEmpty());
            txLatch.countDown();
            tx.set(HELLO_B, "bar".getBytes(StandardCharsets.UTF_8));
            try {
              writeLatch.await();
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            return "written";
          });
    });
    // wait until tx1 has gotten a read version and is about to commit.
    txLatch.await();
    // now in another transaction, cause a write conflict range that overlaps with tx1.
    db.run(tx -> {
      tx.getReadVersion().join();
      tx.addWriteConflictRange(HELLO_B, "hello4".getBytes(StandardCharsets.UTF_8));
      return null;
    });
    writeLatch.countDown();

    assertEquals("not written", cf.join());
    assertNull(getValue(db, "hello".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void testAddWriteConflictKey() throws InterruptedException {
    // clear hello -> hello4
    clearRangeAndCommit(db, "hello".getBytes(StandardCharsets.UTF_8),
        "hello4".getBytes(StandardCharsets.UTF_8));

    CountDownLatch txLatch = new CountDownLatch(1);
    CountDownLatch writeLatch = new CountDownLatch(1);
    AtomicBoolean called = new AtomicBoolean();
    // tx1.
    CompletableFuture<String> cf = db.runAsync(tx -> {
      if (called.getAndSet(true)) {
        return CompletableFuture.completedFuture("not written");
      }
      return tx.getRange(HELLO_B, "hello4".getBytes(StandardCharsets.UTF_8)).asList().
          thenApplyAsync(kvs -> {
            assertTrue(kvs.isEmpty());
            txLatch.countDown();
            tx.set(HELLO_B, "bar".getBytes(StandardCharsets.UTF_8));
            try {
              writeLatch.await();
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            return "written";
          });
    });
    // wait until tx1 has gotten a read version and is about to commit.
    txLatch.await();
    // now in another transaction, cause a write conflict range that overlaps with tx1.
    db.run(tx -> {
      tx.getReadVersion().join();
      tx.addWriteConflictKey(HELLO_B);
      return null;
    });
    writeLatch.countDown();

    assertEquals("not written", cf.join());
    assertNull(getValue(db, "hello".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void testGetKey() {
    setupRangeTest(db);

    // first greater than or equal to hello
    byte[] run = db.run(tx -> tx.getKey(KeySelector.firstGreaterOrEqual(HELLO_B)).join());
    assertArrayEquals(run, HELLO_B);
    run = db.runAsync(tx -> tx.getKey(KeySelector.firstGreaterOrEqual(HELLO_B))).join();
    assertArrayEquals(run, HELLO_B);

    // first greater than hello
    run = db.run(tx -> tx.getKey(KeySelector.firstGreaterThan(HELLO_B)).join());
    byte[] HELLO1_B = "hello1".getBytes(StandardCharsets.UTF_8);
    assertArrayEquals(run, HELLO1_B);
    run = db.runAsync(tx -> tx.getKey(KeySelector.firstGreaterThan(HELLO_B))).join();
    assertArrayEquals(run, HELLO1_B);

    // first greater than or equal to hello0
    byte[] HELLO0_B = "hello0".getBytes(StandardCharsets.UTF_8);
    run = db.run(tx -> tx.getKey(KeySelector.firstGreaterOrEqual(HELLO0_B)).join());
    assertArrayEquals(run, HELLO1_B);
    run = db.runAsync(tx -> tx.getKey(KeySelector.firstGreaterOrEqual(HELLO0_B))).join();
    assertArrayEquals(run, HELLO1_B);

    // first greater than hello
    run = db.run(tx -> tx.getKey(KeySelector.firstGreaterThan(HELLO0_B)).join());
    assertArrayEquals(run, HELLO1_B);
    run = db.runAsync(tx -> tx.getKey(KeySelector.firstGreaterThan(HELLO0_B))).join();
    assertArrayEquals(run, HELLO1_B);

    // less than or equal to hello1
    run = db.run(tx -> tx.getKey(KeySelector.lastLessOrEqual(HELLO1_B)).join());
    assertArrayEquals(run, HELLO1_B);
    run = db.runAsync(tx -> tx.getKey(KeySelector.lastLessOrEqual(HELLO1_B))).join();
    assertArrayEquals(run, HELLO1_B);

    // less than hello1
    run = db.run(tx -> tx.getKey(KeySelector.lastLessThan(HELLO1_B)).join());
    assertArrayEquals(run, HELLO_B);
    run = db.runAsync(tx -> tx.getKey(KeySelector.lastLessThan(HELLO1_B))).join();
    assertArrayEquals(run, HELLO_B);

    // less than or equal to hello0
    run = db.run(tx -> tx.getKey(KeySelector.lastLessOrEqual(HELLO0_B)).join());
    assertArrayEquals(run, HELLO_B);
    run = db.runAsync(tx -> tx.getKey(KeySelector.lastLessOrEqual(HELLO0_B))).join();
    assertArrayEquals(run, HELLO_B);

    // less than hello0
    run = db.run(tx -> tx.getKey(KeySelector.lastLessThan(HELLO0_B)).join());
    assertArrayEquals(run, HELLO_B);
    run = db.runAsync(tx -> tx.getKey(KeySelector.lastLessThan(HELLO0_B))).join();
    assertArrayEquals(run, HELLO_B);
  }

  @Test
  public void testSetReadVersion() {
    // this test is something time sensitive.
    byte[] FOO_B = "foo".getBytes(StandardCharsets.UTF_8);
    long version = db.run(tx -> {
      tx.set(HELLO_B, WORLD_B);
      return tx;
    }).getCommittedVersion();
    long versionNext = db.run(tx -> {
      tx.set(HELLO_B, FOO_B);
      return tx;
    }).getCommittedVersion();

    assertArrayEquals(FOO_B, db.run(tx -> tx.get(HELLO_B).join()));
    assertArrayEquals(WORLD_B, db.run(tx -> {
      tx.setReadVersion(version);
      return tx.get(HELLO_B).join();
    }));
    assertArrayEquals(FOO_B, db.run(tx -> {
      tx.setReadVersion(versionNext);
      return tx.get(HELLO_B).join();
    }));
  }

  @Test
  public void testCancel() {
    Transaction transaction = db.createTransaction();
    transaction.set(HELLO_B, WORLD_B);
    transaction.cancel();
    try {
      transaction.commit();
      fail();
    } catch (IllegalStateException expected) {
    }
  }

  @Test
  public void testRYW() {
    // clear hello -> hello4
    clearRangeAndCommit(db, "hello".getBytes(StandardCharsets.UTF_8),
        "hello4".getBytes(StandardCharsets.UTF_8));
    assertArrayEquals(WORLD_B, db.run(tx -> {
      assertNull(tx.get(HELLO_B).join());
      tx.set(HELLO_B, WORLD_B);
      return tx.get(HELLO_B).join();
    }));
    // clear hello -> hello4
    clearRangeAndCommit(db, "hello".getBytes(StandardCharsets.UTF_8),
        "hello4".getBytes(StandardCharsets.UTF_8));
    assertNull(db.run(tx -> {
      tx.options().setReadYourWritesDisable();
      assertNull(tx.get(HELLO_B).join());
      tx.set(HELLO_B, WORLD_B);
      return tx.get(HELLO_B).join();
    }));
    assertEquals("world", getValue(db, HELLO_B));
  }

  @Test
  public void testGetEstimatedRangeSize() {
    clearRangeAndCommit(db, "hello".getBytes(StandardCharsets.UTF_8),
        "hello4".getBytes(StandardCharsets.UTF_8));
    setupRangeTest(db);
    long size = db.runAsync(tx ->
        tx.getEstimatedRangeSizeBytes(HELLO_B, "hello4".getBytes(StandardCharsets.UTF_8))).join();
    // we can't actually assert the result unfortunately.
    // assertTrue(size > 0);
  }

  @Test
  public void testGetBoundaryKeys() {
    clearRangeAndCommit(db, "hello".getBytes(StandardCharsets.UTF_8),
        "hello4".getBytes(StandardCharsets.UTF_8));
    setupRangeTest(db);
    List<byte[]> join = AsyncUtil.collectRemaining(
        RemoteLocalityUtil.getBoundaryKeys(db, new byte[]{0}, new byte[]{-1})).join();
    // can't assert.
    // assertFalse(join.isEmpty());

    join = db.runAsync(tx -> AsyncUtil.collectRemaining(
        RemoteLocalityUtil.getBoundaryKeys(tx, new byte[]{0}, new byte[]{-1}))).join();
    // can't assert.
    // assertFalse(join.isEmpty());
  }

  @Test
  public void testGetAddressesForKey() {
    clearRangeAndCommit(db, "hello".getBytes(StandardCharsets.UTF_8),
        "hello4".getBytes(StandardCharsets.UTF_8));
    setupRangeTest(db);
    String[] join = db.runAsync(tx -> RemoteLocalityUtil.
        getAddressesForKey(tx, "hello".getBytes(StandardCharsets.UTF_8))).join();
    assertTrue(join.length > 0);
  }
}
