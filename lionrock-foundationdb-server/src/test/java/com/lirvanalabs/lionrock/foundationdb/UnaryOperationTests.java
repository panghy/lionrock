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
class UnaryOperationTests {

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

  @Captor
  ArgumentCaptor<DatabaseResponse> databaseResponseCapture;

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
  void testStartTransaction_withInvalidDatabaseName() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);

    StreamObserver<DatabaseResponse> streamObs = mock(StreamObserver.class);
    stub.execute(DatabaseRequest.newBuilder().setName("testStartTransaction_withInvalidDatabaseName").
        setClientIdentifier("unit test").
        setDatabaseName("NOT_A_VALID_DB").build(), streamObs);

    verify(streamObs, never()).onCompleted();
    verify(streamObs, timeout(5000).times(1)).onError(statusRuntimeExceptionArgumentCaptor.capture());
    StatusRuntimeException value = statusRuntimeExceptionArgumentCaptor.getValue();
    assertEquals(Status.INVALID_ARGUMENT.getCode(), value.getStatus().getCode());
  }

  @Test
  void testSetValue() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);

    // set hello to world
    setKeyAndCommit(stub,
        "hello".getBytes(StandardCharsets.UTF_8),
        "world".getBytes(StandardCharsets.UTF_8));

    // read the key back.
    assertEquals("world", getValue(stub, "hello".getBytes(StandardCharsets.UTF_8)));

    // set the value to something else.
    setKeyAndCommit(stub,
        "hello".getBytes(StandardCharsets.UTF_8),
        "future".getBytes(StandardCharsets.UTF_8));

    // read the key back.
    assertEquals("future", getValue(stub, "hello".getBytes(StandardCharsets.UTF_8)));
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

  @Test
  void testClearRange() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);

    byte[] worldB = "world".getBytes(StandardCharsets.UTF_8);
    setKeyAndCommit(stub, "hello".getBytes(StandardCharsets.UTF_8), worldB);
    setKeyAndCommit(stub, "hello1".getBytes(StandardCharsets.UTF_8), worldB);
    setKeyAndCommit(stub, "hello2".getBytes(StandardCharsets.UTF_8), worldB);
    setKeyAndCommit(stub, "hello3".getBytes(StandardCharsets.UTF_8), worldB);
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

  private void setKeyAndCommit(TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub,
                               byte[] key, byte[] value) {
    StreamObserver<DatabaseResponse> streamObs = mock(StreamObserver.class);

    stub.execute(DatabaseRequest.newBuilder().
        setDatabaseName("fdb").setName("setKeyAndCommit").setClientIdentifier("unit test").
        setSetValue(SetValueRequest.newBuilder().
            setKey(ByteString.copyFrom(key)).
            setValue(ByteString.copyFrom(value)).
            build()).
        build(), streamObs);

    verify(streamObs, timeout(5000).times(1)).onNext(databaseResponseCapture.capture());
    verify(streamObs, timeout(5000).times(1)).onCompleted();
    verify(streamObs, never()).onError(any());
  }

  private void clearKeyAndCommit(TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub,
                                 byte[] key) {
    StreamObserver<DatabaseResponse> streamObs = mock(StreamObserver.class);

    stub.execute(DatabaseRequest.newBuilder().
        setDatabaseName("fdb").setName("clearKeyAndCommit").setClientIdentifier("unit test").
        setClearKey(ClearKeyRequest.newBuilder().
            setKey(ByteString.copyFrom(key)).
            build()).
        build(), streamObs);

    verify(streamObs, timeout(5000).times(1)).onNext(databaseResponseCapture.capture());
    verify(streamObs, timeout(5000).times(1)).onCompleted();
    verify(streamObs, never()).onError(any());
  }

  private void clearRangeAndCommit(TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub,
                                   byte[] start, byte[] end) {
    StreamObserver<DatabaseResponse> streamObs = mock(StreamObserver.class);

    stub.execute(DatabaseRequest.newBuilder().
        setDatabaseName("fdb").setName("clearRangeAndCommit").setClientIdentifier("unit test").
        setClearRange(ClearKeyRangeRequest.newBuilder().
            setStart(ByteString.copyFrom(start)).
            setEnd(ByteString.copyFrom(end)).
            build()).
        build(), streamObs);

    verify(streamObs, timeout(5000).times(1)).onNext(databaseResponseCapture.capture());
    verify(streamObs, timeout(5000).times(1)).onCompleted();
    verify(streamObs, never()).onError(any());
  }

  private String getValue(TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub, byte[] key) {
    StreamObserver<DatabaseResponse> streamObs = mock(StreamObserver.class);

    stub.execute(DatabaseRequest.newBuilder().
        setDatabaseName("fdb").setName("getValue").setClientIdentifier("unit test").
        setGetValue(GetValueRequest.newBuilder().setKey(ByteString.copyFrom(key)).build()).
        build(), streamObs);

    DatabaseResponse value;

    verify(streamObs, timeout(5000).times(1)).onNext(databaseResponseCapture.capture());
    verify(streamObs, timeout(5000).times(1)).onCompleted();
    verify(streamObs, never()).onError(any());

    value = databaseResponseCapture.getValue();
    assertTrue(value.hasGetValue());

    if (!value.getGetValue().hasValue()) {
      return null;
    }
    return value.getGetValue().getValue().toStringUtf8();
  }
}
