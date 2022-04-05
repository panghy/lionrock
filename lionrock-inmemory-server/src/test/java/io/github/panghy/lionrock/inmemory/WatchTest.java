package io.github.panghy.lionrock.inmemory;

import com.google.protobuf.ByteString;
import io.github.panghy.lionrock.proto.*;
import io.github.panghy.lionrock.tests.AbstractStreamingGrpcTest;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

public class WatchTest extends AbstractStreamingGrpcTest {

  @Captor
  ArgumentCaptor<StreamingDatabaseResponse> tx1aCapture;

  @Captor
  ArgumentCaptor<StreamingDatabaseResponse> tx1bCapture;

  /**
   * To setup this test, we run two transactions, one watching a key, and the other changing it..
   */
  @Test
  public void testWatch() {
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
    // watch hello on tx1.
    tx1Stub.onNext(StreamingDatabaseRequest.newBuilder().
        setWatchKey(WatchKeyRequest.newBuilder().
            setSequenceId(12345).
            setKey(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
            build()).
        build());
    // commit tx1.
    tx1Stub.onNext(StreamingDatabaseRequest.newBuilder().setCommitTransaction(
        CommitTransactionRequest.newBuilder().build()
    ).build());
    verify(tx1Observer, timeout(5000).times(1)).onNext(tx1aCapture.capture());
    assertTrue(tx1aCapture.getValue().hasCommitTransaction());
    verify(tx1Observer, never()).onError(any());
    // make sure tx1 doesn't close on the server-side.
    verify(tx1Observer, never()).onCompleted();

    // now in another transaction, write and commit hello.
    setKeyAndCommit(stub, "hello".getBytes(StandardCharsets.UTF_8), new byte[0]);

    // this counts as two calls for mockito (even though we asserted above already).
    verify(tx1Observer, timeout(5000).times(2)).onNext(tx1bCapture.capture());
    assertTrue(tx1bCapture.getAllValues().get(1).hasWatchKey());
    assertEquals(12345, tx1bCapture.getAllValues().get(1).getWatchKey().getSequenceId());
    verify(tx1Observer, never()).onError(any());
    // now it closes.
    tx1Stub.onCompleted();
  }
}
