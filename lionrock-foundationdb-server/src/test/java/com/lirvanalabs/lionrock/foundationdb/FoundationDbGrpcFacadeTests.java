package com.lirvanalabs.lionrock.foundationdb;

import com.google.protobuf.ByteString;
import com.lirvanalabs.lionrock.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lognet.springboot.grpc.GRpcServerRunner;
import org.lognet.springboot.grpc.autoconfigure.GRpcServerProperties;
import org.lognet.springboot.grpc.context.LocalRunningGrpcPort;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.NONE;

@SpringBootTest(webEnvironment = NONE, properties = {"grpc.port=0", "grpc.shutdownGrace=-1"})
class FoundationDbGrpcFacadeTests {

  @Autowired(required = false)
  @Qualifier("grpcServerRunner")
  protected GRpcServerRunner grpcServerRunner;

  @Autowired
  protected GRpcServerProperties gRpcServerProperties;

  @LocalRunningGrpcPort
  protected int runningPort;

  @Captor
  ArgumentCaptor<StatusRuntimeException> statusRuntimeExceptionArgumentCaptor;

  @Captor
  ArgumentCaptor<StreamingDatabaseResponse> streamingDatabaseResponseCapture;

  protected ManagedChannel channel;

  @BeforeEach
  public void setupChannel() throws IOException {
    if (gRpcServerProperties.isEnabled()) {
      ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress("localhost", getPort());
      Resource certChain = Optional.ofNullable(gRpcServerProperties.getSecurity()).
          map(GRpcServerProperties.SecurityProperties::getCertChain).
          orElse(null);
      if (null != certChain) {
        ((NettyChannelBuilder) channelBuilder).
            useTransportSecurity().
            sslContext(GrpcSslContexts.forClient().trustManager(certChain.getInputStream()).build());
      } else {
        channelBuilder.usePlaintext();
      }
      channel = channelBuilder.build();
    }
  }

  @AfterEach
  public void shutdownChannel() {
    Optional.ofNullable(channel).ifPresent(ManagedChannel::shutdownNow);
  }

  protected int getPort() {
    return runningPort;
  }

  @Test
  void testStartTransaction_withValidDatabaseName() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);
    StreamingDatabaseRequest request = StreamingDatabaseRequest.newBuilder().
        setStartTransaction(StartTransactionRequest.newBuilder().
            setName("testStartTransaction_withValidDatabaseName").
            setClientIdentifier("unit test").
            setDatabaseName("fdb").
            build()).
        build();
    StreamObserver<StreamingDatabaseResponse> streamObs = mock(StreamObserver.class);
    StreamObserver<StreamingDatabaseRequest> StreamingDatabaseRequestStreamObserver = stub.executeTransaction(streamObs);
    StreamingDatabaseRequestStreamObserver.onNext(request);
    StreamingDatabaseRequestStreamObserver.onCompleted();

    verify(streamObs, timeout(5000).times(1)).onCompleted();
    verify(streamObs, never()).onError(any());
  }

  @Test
  void testStartTransaction_withInvalidDatabaseName() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);
    StreamingDatabaseRequest request = StreamingDatabaseRequest.newBuilder().
        setStartTransaction(StartTransactionRequest.newBuilder().
            setName("testStartTransaction_withInvalidDatabaseName").
            setClientIdentifier("unit test").
            setDatabaseName("NOT_A_VALID_DB").
            build()).
        build();
    StreamObserver<StreamingDatabaseResponse> streamObs = mock(StreamObserver.class);
    StreamObserver<StreamingDatabaseRequest> StreamingDatabaseRequestStreamObserver = stub.executeTransaction(streamObs);
    StreamingDatabaseRequestStreamObserver.onNext(request);
    StreamingDatabaseRequestStreamObserver.onCompleted();

    verify(streamObs, never()).onCompleted();
    verify(streamObs, timeout(5000).times(1)).onError(statusRuntimeExceptionArgumentCaptor.capture());
    StatusRuntimeException value = statusRuntimeExceptionArgumentCaptor.getValue();
    assertEquals(Status.INVALID_ARGUMENT.getCode(), value.getStatus().getCode());
  }

  @Test
  void testStartTransaction_cannotStartTwice() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);
    StreamingDatabaseRequest request = StreamingDatabaseRequest.newBuilder().
        setStartTransaction(StartTransactionRequest.newBuilder().
            setName("testStartTransaction_cannotStartTwice").
            setClientIdentifier("unit test").
            setDatabaseName("fdb").
            build()).
        build();
    StreamObserver<StreamingDatabaseResponse> streamObs = mock(StreamObserver.class);
    StreamObserver<StreamingDatabaseRequest> StreamingDatabaseRequestStreamObserver = stub.executeTransaction(streamObs);
    StreamingDatabaseRequestStreamObserver.onNext(request);
    StreamingDatabaseRequestStreamObserver.onNext(request);
    StreamingDatabaseRequestStreamObserver.onCompleted();

    verify(streamObs, never()).onCompleted();
    verify(streamObs, timeout(5000).times(1)).onError(statusRuntimeExceptionArgumentCaptor.capture());
    StatusRuntimeException value = statusRuntimeExceptionArgumentCaptor.getValue();
    assertEquals(Status.INVALID_ARGUMENT.getCode(), value.getStatus().getCode());
  }

  @Test
  void testStartTransaction_commitWithNothing() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);

    StreamObserver<StreamingDatabaseResponse> streamObs = mock(StreamObserver.class);
    StreamObserver<StreamingDatabaseRequest> streamingDatabaseRequestStreamObserver = stub.executeTransaction(streamObs);
    streamingDatabaseRequestStreamObserver.onNext(StreamingDatabaseRequest.newBuilder().
        setStartTransaction(StartTransactionRequest.newBuilder().
            setName("testStartTransaction_commitWithNothing").
            setClientIdentifier("unit test").
            setDatabaseName("fdb").
            build()).
        build());
    streamingDatabaseRequestStreamObserver.onNext(StreamingDatabaseRequest.newBuilder().
        setCommitTransaction(CommitTransactionRequest.newBuilder().build()).
        build());

    verify(streamObs, timeout(5000).times(1)).onNext(streamingDatabaseResponseCapture.capture());
    streamingDatabaseRequestStreamObserver.onCompleted();
    verify(streamObs, timeout(5000).times(1)).onCompleted();
    verify(streamObs, never()).onError(any());

    StreamingDatabaseResponse value = streamingDatabaseResponseCapture.getValue();
    assertTrue(value.hasCommitTransaction());
    assertEquals(-1, value.getCommitTransaction().getCommittedVersion());
  }

  @Test
  void testSetValue() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);

    // set hello to world
    long lastCommittedVersion = setKeyAndCommit(stub,
        "hello".getBytes(StandardCharsets.UTF_8),
        "world".getBytes(StandardCharsets.UTF_8));

    // read the key back.
    assertEquals("world", getValue(stub, "hello".getBytes(StandardCharsets.UTF_8)));

    // set the value to something else.
    assertTrue(setKeyAndCommit(stub,
        "hello".getBytes(StandardCharsets.UTF_8),
        "future".getBytes(StandardCharsets.UTF_8)) > lastCommittedVersion);

    // read the key back.
    assertEquals("future", getValue(stub, "hello".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void testReadAndTimeout() throws InterruptedException {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);

    StreamObserver<StreamingDatabaseResponse> streamObs = mock(StreamObserver.class);

    StreamObserver<StreamingDatabaseRequest> serverStub;
    serverStub = stub.executeTransaction(streamObs);
    serverStub.onNext(StreamingDatabaseRequest.newBuilder().
        setStartTransaction(StartTransactionRequest.newBuilder().
            setName("testReadAndTimeout").
            setClientIdentifier("unit test").
            setDatabaseName("fdb").
            build()).
        build());
    // read any key to trigger GRV.
    serverStub.onNext(StreamingDatabaseRequest.newBuilder().
        setGetValue(GetValueRequest.newBuilder().
            setKey(ByteString.copyFrom("dummy".getBytes(StandardCharsets.UTF_8))).build()).
        build());

    // sleep for 5s.
    Thread.sleep(6000);

    serverStub.onNext(StreamingDatabaseRequest.newBuilder().
        setSetValue(SetValueRequest.newBuilder().
            setKey(ByteString.copyFrom("dummy".getBytes(StandardCharsets.UTF_8))).
            setValue(ByteString.copyFrom("dummy".getBytes(StandardCharsets.UTF_8))).
            build()).
        build());
    serverStub.onNext(StreamingDatabaseRequest.newBuilder().
        setCommitTransaction(CommitTransactionRequest.newBuilder().build()).
        build());

    verify(streamObs, never()).onCompleted();
    verify(streamObs, timeout(5000).times(1)).onError(statusRuntimeExceptionArgumentCaptor.capture());
    StatusRuntimeException value = statusRuntimeExceptionArgumentCaptor.getValue();
    assertEquals(Status.ABORTED.getCode(), value.getStatus().getCode());
  }

  @Test
  void testClearKey() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);

    byte[] helloB = "hello".getBytes(StandardCharsets.UTF_8);
    byte[] worldB = "world".getBytes(StandardCharsets.UTF_8);
    setKeyAndCommit(stub, helloB, worldB);
    assertEquals("world", getValue(stub, helloB));

    clearKeyAndCommit(stub, helloB);
    assertNull(getValue(stub, helloB));
  }

  private long setKeyAndCommit(TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub,
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
        setCommitTransaction(CommitTransactionRequest.newBuilder().build()).
        build());

    verify(streamObs, timeout(5000).times(1)).onNext(streamingDatabaseResponseCapture.capture());
    serverStub.onCompleted();
    verify(streamObs, timeout(5000).times(1)).onCompleted();
    verify(streamObs, never()).onError(any());

    response = streamingDatabaseResponseCapture.getValue();
    assertTrue(response.hasCommitTransaction());

    return response.getCommitTransaction().getCommittedVersion();
  }

  private long clearKeyAndCommit(TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub,
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
    verify(streamObs, timeout(5000).times(1)).onCompleted();
    verify(streamObs, never()).onError(any());

    response = streamingDatabaseResponseCapture.getValue();
    assertTrue(response.hasCommitTransaction());

    return response.getCommitTransaction().getCommittedVersion();
  }

  private String getValue(TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub, byte[] key) {
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
    verify(streamObs, timeout(5000).times(1)).onCompleted();
    verify(streamObs, never()).onError(any());

    value = streamingDatabaseResponseCapture.getValue();
    assertTrue(value.hasGetValue());

    if (!value.getGetValue().hasValue()) {
      return null;
    }
    return value.getGetValue().getValue().toStringUtf8();
  }
}
