package io.github.panghy.lionrock.tests;

import com.google.protobuf.ByteString;
import io.github.panghy.lionrock.proto.*;
import io.grpc.stub.StreamObserver;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class AbstractStreamingGrpcTest extends AbstractGrpcTest {

  protected byte[] getKey(TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub, KeySelector keySelector) {
    StreamObserver<StreamingDatabaseResponse> streamObs = mock(StreamObserver.class);
    StreamObserver<StreamingDatabaseRequest> serverStub;
    serverStub = stub.executeTransaction(streamObs);
    serverStub.onNext(StreamingDatabaseRequest.newBuilder().
        setStartTransaction(StartTransactionRequest.newBuilder().
            setName("getRange").
            setClientIdentifier("unit test").
            setDatabaseName("fdb").
            build()).
        build());
    serverStub.onNext(StreamingDatabaseRequest.newBuilder().
        setGetKey(GetKeyRequest.newBuilder().
            setKeySelector(keySelector).
            build()).
        build());

    verify(streamObs, timeout(5000).times(1)).onNext(streamingDatabaseResponseCapture.capture());
    serverStub.onCompleted();
    verify(streamObs, never()).onError(any());

    StreamingDatabaseResponse value = streamingDatabaseResponseCapture.getValue();
    assertTrue(value.hasGetKey());
    return value.getGetKey().hasKey() ? value.getGetKey().getKey().toByteArray() : null;
  }

  protected List<KeyValue> getRange(TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub,
                                    KeySelector start, KeySelector end, int limit, boolean reverse) {
    StreamObserver<StreamingDatabaseResponse> streamObs = mock(StreamObserver.class);

    StreamObserver<StreamingDatabaseRequest> serverStub;
    serverStub = stub.executeTransaction(streamObs);
    serverStub.onNext(StreamingDatabaseRequest.newBuilder().
        setStartTransaction(StartTransactionRequest.newBuilder().
            setName("getRange").
            setClientIdentifier("unit test").
            setDatabaseName("fdb").
            build()).
        build());
    serverStub.onNext(StreamingDatabaseRequest.newBuilder().
        setGetRange(GetRangeRequest.newBuilder().
            setStartKeySelector(start).
            setEndKeySelector(end).
            setLimit(limit).
            setReverse(reverse).
            setStreamingMode(StreamingMode.WANT_ALL).
            build()).
        build());

    verify(streamObs, timeout(5000).times(1)).onNext(streamingDatabaseResponseCapture.capture());
    serverStub.onCompleted();
    verify(streamObs, never()).onError(any());

    StreamingDatabaseResponse value = streamingDatabaseResponseCapture.getValue();
    assertTrue(value.hasGetRange());
    assertTrue(value.getGetRange().getDone());

    return value.getGetRange().getKeyValuesList();
  }

  protected long setKeyAndCommit(TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub,
                                 byte[] key, byte[] value) {
    StreamObserver<StreamingDatabaseResponse> streamObs = mock(StreamObserver.class);

    StreamingDatabaseResponse response;
    StreamObserver<StreamingDatabaseRequest> serverStub;
    serverStub = stub.executeTransaction(streamObs);
    serverStub.onNext(StreamingDatabaseRequest.newBuilder().
        setStartTransaction(StartTransactionRequest.newBuilder().
            setName("setKeyAndCommit").
            setClientIdentifier("unit test").
            setDatabaseName("fdb").
            build()).
        build());
    serverStub.onNext(StreamingDatabaseRequest.newBuilder().
        setSetValue(SetValueRequest.newBuilder().
            setKey(ByteString.copyFrom(key)).
            setValue(ByteString.copyFrom(value)).
            build()).
        build());
    serverStub.onNext(StreamingDatabaseRequest.newBuilder().
        setGetApproximateSize(GetApproximateSizeRequest.newBuilder().setSequenceId(123).build()).
        build());

    // check approximate size.
    verify(streamObs, timeout(5000).times(1)).onNext(streamingDatabaseResponseCapture.capture());
    response = streamingDatabaseResponseCapture.getValue();
    assertTrue(response.hasGetApproximateSize());
    assertTrue(response.getGetApproximateSize().getSize() > 0);
    assertEquals(123, response.getGetApproximateSize().getSequenceId());

    // commit.
    serverStub.onNext(StreamingDatabaseRequest.newBuilder().
        setCommitTransaction(CommitTransactionRequest.newBuilder().build()).
        build());

    verify(streamObs, timeout(5000).times(2)).onNext(streamingDatabaseResponseCapture.capture());
    serverStub.onCompleted();
    verify(streamObs, never()).onError(any());

    response = streamingDatabaseResponseCapture.getValue();
    assertTrue(response.hasCommitTransaction());

    return response.getCommitTransaction().getCommittedVersion();
  }

  protected long clearKeyAndCommit(TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub,
                                   byte[] key) {
    StreamObserver<StreamingDatabaseResponse> streamObs = mock(StreamObserver.class);

    StreamingDatabaseResponse response;
    StreamObserver<StreamingDatabaseRequest> serverStub;
    serverStub = stub.executeTransaction(streamObs);
    serverStub.onNext(StreamingDatabaseRequest.newBuilder().
        setStartTransaction(StartTransactionRequest.newBuilder().
            setName("setKeyAndCommit").
            setClientIdentifier("unit test").
            setDatabaseName("fdb").
            build()).
        build());
    serverStub.onNext(StreamingDatabaseRequest.newBuilder().
        setClearKey(ClearKeyRequest.newBuilder().
            setKey(ByteString.copyFrom(key)).
            build()).
        build());
    serverStub.onNext(StreamingDatabaseRequest.newBuilder().
        setCommitTransaction(CommitTransactionRequest.newBuilder().build()).
        build());

    verify(streamObs, timeout(5000).times(1)).onNext(streamingDatabaseResponseCapture.capture());
    serverStub.onCompleted();
    verify(streamObs, never()).onError(any());

    response = streamingDatabaseResponseCapture.getValue();
    assertTrue(response.hasCommitTransaction());

    return response.getCommitTransaction().getCommittedVersion();
  }

  protected long clearRangeAndCommit(TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub,
                                     byte[] start, byte[] end) {
    StreamObserver<StreamingDatabaseResponse> streamObs = mock(StreamObserver.class);

    StreamingDatabaseResponse response;
    StreamObserver<StreamingDatabaseRequest> serverStub;
    serverStub = stub.executeTransaction(streamObs);
    serverStub.onNext(StreamingDatabaseRequest.newBuilder().
        setStartTransaction(StartTransactionRequest.newBuilder().
            setName("setKeyAndCommit").
            setClientIdentifier("unit test").
            setDatabaseName("fdb").
            build()).
        build());
    serverStub.onNext(StreamingDatabaseRequest.newBuilder().
        setClearRange(ClearKeyRangeRequest.newBuilder().
            setStart(ByteString.copyFrom(start)).
            setEnd(ByteString.copyFrom(end)).
            build()).
        build());
    serverStub.onNext(StreamingDatabaseRequest.newBuilder().
        setCommitTransaction(CommitTransactionRequest.newBuilder().build()).
        build());

    verify(streamObs, timeout(5000).times(1)).onNext(streamingDatabaseResponseCapture.capture());
    serverStub.onCompleted();
    verify(streamObs, never()).onError(any());

    response = streamingDatabaseResponseCapture.getValue();
    assertTrue(response.hasCommitTransaction());

    return response.getCommitTransaction().getCommittedVersion();
  }

  protected String getValue(TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub, byte[] key) {
    StreamObserver<StreamingDatabaseResponse> streamObs = mock(StreamObserver.class);

    StreamingDatabaseResponse value;
    StreamObserver<StreamingDatabaseRequest> serverStub;
    serverStub = stub.executeTransaction(streamObs);
    serverStub.onNext(StreamingDatabaseRequest.newBuilder().
        setStartTransaction(StartTransactionRequest.newBuilder().
            setName("getValue").
            setClientIdentifier("unit test").
            setDatabaseName("fdb").
            build()).
        build());
    serverStub.onNext(StreamingDatabaseRequest.newBuilder().
        setGetValue(GetValueRequest.newBuilder().setKey(ByteString.copyFrom(key)).build()).
        build());

    verify(streamObs, timeout(5000).times(1)).onNext(streamingDatabaseResponseCapture.capture());
    serverStub.onCompleted();
    verify(streamObs, never()).onError(any());

    value = streamingDatabaseResponseCapture.getValue();
    assertTrue(value.hasGetValue());

    if (!value.getGetValue().hasValue()) {
      return null;
    }
    return value.getGetValue().getValue().toStringUtf8();
  }
}
