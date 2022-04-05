package io.github.panghy.lionrock.foundationdb;

import com.google.protobuf.ByteString;
import io.github.panghy.lionrock.proto.*;
import io.github.panghy.lionrock.tests.AbstractStreamingGrpcTest;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class WriteConflictTest extends AbstractStreamingGrpcTest {

  /**
   * To setup this test, we run two transactions, one performing a blind write against a range that was read first by
   * another one. Since blind writes should always go through, we expect the read-then-write transaction to fail.
   */
  @Test
  public void testWriteConflict() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);
    // clear hello -> hello4
    clearRangeAndCommit(stub, "hello".getBytes(StandardCharsets.UTF_8),
        "hello4".getBytes(StandardCharsets.UTF_8));

    StreamObserver<StreamingDatabaseResponse> tx1Observer = mock(StreamObserver.class);

    StreamObserver<StreamingDatabaseRequest> tx1Stub = stub.executeTransaction(tx1Observer);
    tx1Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setStartTransaction(StartTransactionRequest.newBuilder().
            setName("read-then-write").
            setClientIdentifier("unit test").
            setDatabaseName("fdb").
            build()).
        build());
    tx1Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setGetRange(GetRangeRequest.newBuilder().
            setStartBytes(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
            setEndBytes(ByteString.copyFrom("hello4", StandardCharsets.UTF_8)).build()).
        build());
    // expected to receive no rows.
    verify(tx1Observer, timeout(10000).times(1)).onNext(streamingDatabaseResponseCapture.capture());
    StreamingDatabaseResponse resp = streamingDatabaseResponseCapture.getValue();
    assertTrue(resp.getGetRange().getDone());
    assertTrue(resp.getGetRange().getKeyValuesList().isEmpty());
    // now write something within the range [hello, hello4) but don't commit.
    tx1Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setSetValue(SetValueRequest.newBuilder().
            setKey(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
            setValue(ByteString.EMPTY).build()).
        build());

    // now in another transaction, write and commit hello2.
    setKeyAndCommit(stub, "hello2".getBytes(StandardCharsets.UTF_8), new byte[0]);

    // now if we commit tx1, it should conflict and fail.
    tx1Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setCommitTransaction(CommitTransactionRequest.newBuilder().build()).build());

    verify(tx1Observer, timeout(10000).times(1)).onError(any());
    // make sure we didn't commit "hello".
    assertNull(getValue(stub, "hello".getBytes(StandardCharsets.UTF_8)));
    // and that we did commit "hello2".
    assertNotNull(getValue(stub, "hello2".getBytes(StandardCharsets.UTF_8)));
  }

  /**
   * To setup this test, we run two transactions, one performing a blind write against a range that was read by
   * another one (via conflict range read). Since blind writes should always go through, we expect the read-then-write
   * transaction to fail.
   */
  @Test
  public void testAddReadConflictRange() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);
    // clear hello -> hello4
    clearRangeAndCommit(stub, "hello".getBytes(StandardCharsets.UTF_8),
        "hello4".getBytes(StandardCharsets.UTF_8));

    StreamObserver<StreamingDatabaseResponse> tx1Observer = mock(StreamObserver.class);

    StreamObserver<StreamingDatabaseRequest> tx1Stub = stub.executeTransaction(tx1Observer);
    tx1Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setStartTransaction(StartTransactionRequest.newBuilder().
            setName("read-then-write").
            setClientIdentifier("unit test").
            setDatabaseName("fdb").
            build()).
        build());
    // need to get a read version to make sure we conflict
    tx1Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setGetReadVersion(GetReadVersionRequest.newBuilder().build()).
        build());
    verify(tx1Observer, timeout(10000).times(1)).onNext(streamingDatabaseResponseCapture.capture());
    StreamingDatabaseResponse resp = streamingDatabaseResponseCapture.getValue();
    assertTrue(resp.hasGetReadVersion());

    tx1Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setAddConflictRange(AddConflictRangeRequest.newBuilder().
            setStart(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
            setEnd(ByteString.copyFrom("hello4", StandardCharsets.UTF_8)).build()).
        build());
    // now write something within the range [hello, hello4) but don't commit.
    tx1Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setSetValue(SetValueRequest.newBuilder().
            setKey(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
            setValue(ByteString.EMPTY).build()).
        build());

    // now in another transaction, write and commit hello2.
    setKeyAndCommit(stub, "hello2".getBytes(StandardCharsets.UTF_8), new byte[0]);

    // now if we commit tx1, it should conflict and fail.
    tx1Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setCommitTransaction(CommitTransactionRequest.newBuilder().build()).build());

    verify(tx1Observer, timeout(10000).times(1)).onError(any());
    // make sure we didn't commit "hello".
    assertNull(getValue(stub, "hello".getBytes(StandardCharsets.UTF_8)));
    // and that we did commit "hello2".
    assertNotNull(getValue(stub, "hello2".getBytes(StandardCharsets.UTF_8)));
  }

  /**
   * To setup this test, we run two transactions, one performing a blind write against a key that was read by
   * another one (via conflict range read). Since blind writes should always go through, we expect the read-then-write
   * transaction to fail.
   */
  @Test
  public void testAddReadConflictKey() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);
    // clear hello -> hello4
    clearRangeAndCommit(stub, "hello".getBytes(StandardCharsets.UTF_8),
        "hello4".getBytes(StandardCharsets.UTF_8));

    StreamObserver<StreamingDatabaseResponse> tx1Observer = mock(StreamObserver.class);

    StreamObserver<StreamingDatabaseRequest> tx1Stub = stub.executeTransaction(tx1Observer);
    tx1Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setStartTransaction(StartTransactionRequest.newBuilder().
            setName("read-then-write").
            setClientIdentifier("unit test").
            setDatabaseName("fdb").
            build()).
        build());
    // need to get a read version to make sure we conflict
    tx1Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setGetReadVersion(GetReadVersionRequest.newBuilder().build()).
        build());
    verify(tx1Observer, timeout(10000).times(1)).onNext(streamingDatabaseResponseCapture.capture());
    StreamingDatabaseResponse resp = streamingDatabaseResponseCapture.getValue();
    assertTrue(resp.hasGetReadVersion());

    tx1Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setAddConflictKey(AddConflictKeyRequest.newBuilder().
            setKey(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).build()).
        build());
    // now write to hello.
    tx1Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setSetValue(SetValueRequest.newBuilder().
            setKey(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom("world", StandardCharsets.UTF_8)).build()).
        build());

    // now in another transaction, write and commit hello2.
    setKeyAndCommit(stub, "hello".getBytes(StandardCharsets.UTF_8), new byte[0]);

    // now if we commit tx1, it should conflict and fail.
    tx1Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setCommitTransaction(CommitTransactionRequest.newBuilder().build()).build());

    verify(tx1Observer, timeout(10000).times(1)).onError(any());
    // and that we did commit "hello" (with the empty string).
    assertEquals("", getValue(stub, "hello".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void testAddWriteConflictRange() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);
    // clear hello -> hello4
    clearRangeAndCommit(stub, "hello".getBytes(StandardCharsets.UTF_8),
        "hello4".getBytes(StandardCharsets.UTF_8));

    StreamObserver<StreamingDatabaseResponse> tx1Observer = mock(StreamObserver.class);

    StreamObserver<StreamingDatabaseRequest> tx1Stub = stub.executeTransaction(tx1Observer);
    tx1Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setStartTransaction(StartTransactionRequest.newBuilder().
            setName("read-then-write").
            setClientIdentifier("unit test").
            setDatabaseName("fdb").
            build()).
        build());
    tx1Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setGetRange(GetRangeRequest.newBuilder().
            setStartBytes(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
            setEndBytes(ByteString.copyFrom("hello4", StandardCharsets.UTF_8)).build()).
        build());
    // expected to receive no rows.
    verify(tx1Observer, timeout(10000).times(1)).onNext(streamingDatabaseResponseCapture.capture());
    StreamingDatabaseResponse resp = streamingDatabaseResponseCapture.getValue();
    assertTrue(resp.getGetRange().getDone());
    assertTrue(resp.getGetRange().getKeyValuesList().isEmpty());
    // now write something within the range [hello, hello4) but don't commit.
    tx1Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setSetValue(SetValueRequest.newBuilder().
            setKey(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
            setValue(ByteString.EMPTY).build()).
        build());

    // now in another transaction, cause a write conflict range that overlaps with tx1.
    StreamObserver<StreamingDatabaseResponse> tx2Observer = mock(StreamObserver.class);
    StreamObserver<StreamingDatabaseRequest> tx2Stub = stub.executeTransaction(tx2Observer);
    tx2Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setStartTransaction(StartTransactionRequest.newBuilder().
            setName("conflict-write").
            setClientIdentifier("unit test").
            setDatabaseName("fdb").
            build()).
        build());
    // need to get a read version to make sure we conflict
    tx2Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setGetReadVersion(GetReadVersionRequest.newBuilder().build()).
        build());
    verify(tx2Observer, timeout(10000).times(1)).onNext(streamingDatabaseResponseCapture.capture());
    resp = streamingDatabaseResponseCapture.getValue();
    assertTrue(resp.hasGetReadVersion());
    tx2Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setAddConflictRange(AddConflictRangeRequest.newBuilder().
            setStart(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
            setEnd(ByteString.copyFrom("hello4", StandardCharsets.UTF_8)).
            setWrite(true).
            build()).
        build());
    // commit that transaction
    tx2Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setCommitTransaction(CommitTransactionRequest.newBuilder().build()).
        build());
    verify(tx2Observer, timeout(10000).times(2)).onNext(streamingDatabaseResponseCapture.capture());
    tx2Stub.onCompleted();
    verify(tx2Observer, never()).onError(any());

    // now if we commit tx1, it should conflict and fail.
    tx1Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setCommitTransaction(CommitTransactionRequest.newBuilder().build()).build());

    verify(tx1Observer, timeout(10000).times(1)).onError(any());
    // make sure we didn't commit "hello".
    assertNull(getValue(stub, "hello".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void testAddWriteConflictKey() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);
    // clear hello -> hello4
    clearRangeAndCommit(stub, "hello".getBytes(StandardCharsets.UTF_8),
        "hello4".getBytes(StandardCharsets.UTF_8));

    StreamObserver<StreamingDatabaseResponse> tx1Observer = mock(StreamObserver.class);

    StreamObserver<StreamingDatabaseRequest> tx1Stub = stub.executeTransaction(tx1Observer);
    tx1Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setStartTransaction(StartTransactionRequest.newBuilder().
            setName("read-then-write").
            setClientIdentifier("unit test").
            setDatabaseName("fdb").
            build()).
        build());
    tx1Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setGetValue(GetValueRequest.newBuilder().
            setKey(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).build()).
        build());
    // expected to receive null.
    verify(tx1Observer, timeout(10000).times(1)).onNext(streamingDatabaseResponseCapture.capture());
    StreamingDatabaseResponse resp = streamingDatabaseResponseCapture.getValue();
    assertTrue(resp.hasGetValue());
    assertFalse(resp.getGetValue().hasValue());
    tx1Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setSetValue(SetValueRequest.newBuilder().
            setKey(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
            setValue(ByteString.EMPTY).build()).
        build());

    // now in another transaction, cause a write conflict range that overlaps with tx1.
    StreamObserver<StreamingDatabaseResponse> tx2Observer = mock(StreamObserver.class);
    StreamObserver<StreamingDatabaseRequest> tx2Stub = stub.executeTransaction(tx2Observer);
    tx2Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setStartTransaction(StartTransactionRequest.newBuilder().
            setName("conflict-write").
            setClientIdentifier("unit test").
            setDatabaseName("fdb").
            build()).
        build());
    // need to get a read version to make sure we conflict
    tx2Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setGetReadVersion(GetReadVersionRequest.newBuilder().build()).
        build());
    verify(tx2Observer, timeout(10000).times(1)).onNext(streamingDatabaseResponseCapture.capture());
    resp = streamingDatabaseResponseCapture.getValue();
    assertTrue(resp.hasGetReadVersion());
    tx2Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setAddConflictKey(AddConflictKeyRequest.newBuilder().
            setKey(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
            setWrite(true).
            build()).
        build());
    // commit that transaction
    tx2Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setCommitTransaction(CommitTransactionRequest.newBuilder().build()).
        build());
    verify(tx2Observer, timeout(10000).times(2)).onNext(streamingDatabaseResponseCapture.capture());
    tx2Stub.onCompleted();
    verify(tx2Observer, never()).onError(any());

    // now if we commit tx1, it should conflict and fail.
    tx1Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setCommitTransaction(CommitTransactionRequest.newBuilder().build()).build());

    verify(tx1Observer, timeout(10000).times(1)).onError(any());
    // make sure we didn't commit "hello".
    assertNull(getValue(stub, "hello".getBytes(StandardCharsets.UTF_8)));
  }
}
