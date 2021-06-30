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
import org.junit.jupiter.api.Disabled;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.NONE;

@SpringBootTest(webEnvironment = NONE, properties = {"grpc.port=0", "grpc.shutdownGrace=-1"})
class BidirectionalStreamingTests {

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

  /**
   * Test is flaky, FDB does allow commit on a transaction that's older than 5s.
   *
   * <code>
   * 2021-06-27 20:39:34.628 DEBUG [fdb-facade,b0cd6877ed1bc713,fe7fab461c7133e8] 51551 --- [     fdb-java-2] c.l.l.f.FoundationDbGrpcFacade           : GetValueRequest on: dummy is: dummy
   * 2021-06-27 20:39:40.641 DEBUG [fdb-facade,b0cd6877ed1bc713,b0cd6877ed1bc713] 51551 --- [ault-executor-2] c.l.l.f.FoundationDbGrpcFacade           : SetValueRequest for: dummy => dummy
   * 2021-06-27 20:39:40.642 DEBUG [fdb-facade,b0cd6877ed1bc713,b0cd6877ed1bc713] 51551 --- [ault-executor-2] c.l.l.f.FoundationDbGrpcFacade           : CommitTransactionRequest
   * </code>
   */
  @Disabled
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
    // wait for the read callback.
    verify(streamObs, timeout(5000).times(1)).onNext(streamingDatabaseResponseCapture.capture());

    // sleep for 6s.
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

  @Test
  void testClearRange() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);

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

  @Test
  public void testGetRange_explicitStartEnd() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);

    byte[] worldB = setupRangeTest(stub);

    // firstAfterHello
    List<KeyValue> results = getRange(stub, equals("hello".getBytes(StandardCharsets.UTF_8)),
        equals("hello3".getBytes(StandardCharsets.UTF_8)), 0, false);
    assertEquals(3, results.size());
    assertTrue(results.contains(
        KeyValue.newBuilder().setKey(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom(worldB)).build()
    ));
    assertTrue(results.contains(
        KeyValue.newBuilder().setKey(ByteString.copyFrom("hello1", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom(worldB)).build()
    ));
    assertTrue(results.contains(
        KeyValue.newBuilder().setKey(ByteString.copyFrom("hello2", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom(worldB)).build()
    ));
  }

  @Test
  public void testGetRange_keyselectorStart_firstGreaterThan() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);

    byte[] worldB = setupRangeTest(stub);

    // firstAfter hello
    List<KeyValue> results = getRange(stub, keySelector("hello".getBytes(StandardCharsets.UTF_8), 1, true),
        equals("hello3".getBytes(StandardCharsets.UTF_8)), 0, false);
    assertEquals(2, results.size());
    assertTrue(results.contains(
        KeyValue.newBuilder().setKey(ByteString.copyFrom("hello1", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom(worldB)).build()
    ));
    assertTrue(results.contains(
        KeyValue.newBuilder().setKey(ByteString.copyFrom("hello2", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom(worldB)).build()
    ));
  }

  @Test
  public void testGetRange_keyselectorStart_firstGreaterThanOrEqual() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);

    byte[] worldB = setupRangeTest(stub);

    // firstAfterOrEqual to Hello
    List<KeyValue> results = getRange(stub, keySelector("hello".getBytes(StandardCharsets.UTF_8), 1, false),
        equals("hello3".getBytes(StandardCharsets.UTF_8)), 0, false);
    assertEquals(3, results.size());
    assertTrue(results.contains(
        KeyValue.newBuilder().setKey(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom(worldB)).build()
    ));
    assertTrue(results.contains(
        KeyValue.newBuilder().setKey(ByteString.copyFrom("hello1", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom(worldB)).build()
    ));
    assertTrue(results.contains(
        KeyValue.newBuilder().setKey(ByteString.copyFrom("hello2", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom(worldB)).build()
    ));
  }

  @Test
  public void testGetRange_keyselectorStart_lastLessThanOrEqual() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);

    byte[] worldB = setupRangeTest(stub);

    // lastLessThanOrEqual "hello"
    List<KeyValue> results = getRange(stub, keySelector("hello".getBytes(StandardCharsets.UTF_8), 0, true),
        equals("hello3".getBytes(StandardCharsets.UTF_8)), 0, false);
    assertEquals(3, results.size());
    assertTrue(results.contains(
        KeyValue.newBuilder().setKey(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom(worldB)).build()
    ));
    assertTrue(results.contains(
        KeyValue.newBuilder().setKey(ByteString.copyFrom("hello1", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom(worldB)).build()
    ));
    assertTrue(results.contains(
        KeyValue.newBuilder().setKey(ByteString.copyFrom("hello2", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom(worldB)).build()
    ));
  }

  @Test
  public void testGetRange_keyselectorStart_lastLessThan() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);

    byte[] worldB = setupRangeTest(stub);

    // lastLessThan hello0 which is hello.
    List<KeyValue> results = getRange(stub, keySelector("hello0".getBytes(StandardCharsets.UTF_8), 0, false),
        equals("hello3".getBytes(StandardCharsets.UTF_8)), 0, false);
    assertEquals(3, results.size());
    assertTrue(results.contains(
        KeyValue.newBuilder().setKey(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom(worldB)).build()
    ));
    assertTrue(results.contains(
        KeyValue.newBuilder().setKey(ByteString.copyFrom("hello1", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom(worldB)).build()
    ));
    assertTrue(results.contains(
        KeyValue.newBuilder().setKey(ByteString.copyFrom("hello2", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom(worldB)).build()
    ));
  }

  @Test
  public void testGetRange_keyselectorEnd_firstGreaterThan() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);

    byte[] worldB = setupRangeTest(stub);

    // firstAfter hello2
    List<KeyValue> results = getRange(stub, equals("hello".getBytes(StandardCharsets.UTF_8)),
        keySelector("hello2".getBytes(StandardCharsets.UTF_8), 1, true), 0, false);
    assertEquals(3, results.size());
    assertTrue(results.contains(
        KeyValue.newBuilder().setKey(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom(worldB)).build()
    ));
    assertTrue(results.contains(
        KeyValue.newBuilder().setKey(ByteString.copyFrom("hello1", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom(worldB)).build()
    ));
    assertTrue(results.contains(
        KeyValue.newBuilder().setKey(ByteString.copyFrom("hello2", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom(worldB)).build()
    ));
  }

  @Test
  public void testGetRange_keyselectorEnd_firstGreaterThanOrEqual() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);

    byte[] worldB = setupRangeTest(stub);

    // firstAfterOrEqual to Hello
    List<KeyValue> results = getRange(stub, equals("hello".getBytes(StandardCharsets.UTF_8)),
        keySelector("hello3".getBytes(StandardCharsets.UTF_8), 1, false), 0, false);
    assertEquals(3, results.size());
    assertTrue(results.contains(
        KeyValue.newBuilder().setKey(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom(worldB)).build()
    ));
    assertTrue(results.contains(
        KeyValue.newBuilder().setKey(ByteString.copyFrom("hello1", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom(worldB)).build()
    ));
    assertTrue(results.contains(
        KeyValue.newBuilder().setKey(ByteString.copyFrom("hello2", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom(worldB)).build()
    ));
  }

  @Test
  public void testGetRange_keyselectorEnd_lastLessThanOrEqual() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);

    byte[] worldB = setupRangeTest(stub);

    // lastLessThanOrEqual "hello"
    List<KeyValue> results = getRange(stub, equals("hello".getBytes(StandardCharsets.UTF_8)),
        keySelector("hello3".getBytes(StandardCharsets.UTF_8), 0, true), 0, false);
    assertEquals(3, results.size());
    assertTrue(results.contains(
        KeyValue.newBuilder().setKey(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom(worldB)).build()
    ));
    assertTrue(results.contains(
        KeyValue.newBuilder().setKey(ByteString.copyFrom("hello1", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom(worldB)).build()
    ));
    assertTrue(results.contains(
        KeyValue.newBuilder().setKey(ByteString.copyFrom("hello2", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom(worldB)).build()
    ));
  }

  @Test
  public void testGetRange_keyselectorEnd_lastLessThan() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);

    byte[] worldB = setupRangeTest(stub);

    // lastLessThan hello3 which is hello2.
    List<KeyValue> results = getRange(stub, equals("hello".getBytes(StandardCharsets.UTF_8)),
        keySelector("hello3".getBytes(StandardCharsets.UTF_8), 0, false), 0, false);
    assertEquals(2, results.size());
    assertTrue(results.contains(
        KeyValue.newBuilder().setKey(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom(worldB)).build()
    ));
    assertTrue(results.contains(
        KeyValue.newBuilder().setKey(ByteString.copyFrom("hello1", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom(worldB)).build()
    ));
  }

  @Test
  public void testLargeGetRange() throws InterruptedException {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);

    clearRangeAndCommit(stub, "hello".getBytes(StandardCharsets.UTF_8),
        "hello10000".getBytes(StandardCharsets.UTF_8));

    // setup 10k rows.
    byte[] worldB = "world".getBytes(StandardCharsets.UTF_8);
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
    for (int i = 0; i < 10000; i++) {
      serverStub.onNext(StreamingDatabaseRequest.newBuilder().
          setSetValue(SetValueRequest.newBuilder().
              setKey(ByteString.copyFrom(("hello" + i).getBytes(StandardCharsets.UTF_8))).
              setValue(ByteString.copyFrom(worldB)).
              build()).
          build());
    }
    serverStub.onNext(StreamingDatabaseRequest.newBuilder().
        setCommitTransaction(CommitTransactionRequest.newBuilder().build()).
        build());

    verify(streamObs, timeout(5000).times(1)).onNext(streamingDatabaseResponseCapture.capture());
    serverStub.onCompleted();
    verify(streamObs, timeout(5000).times(1)).onCompleted();
    verify(streamObs, never()).onError(any());

    response = streamingDatabaseResponseCapture.getValue();
    assertTrue(response.hasCommitTransaction());

    CountDownLatch latch = new CountDownLatch(1);
    List<String> keyValues = new ArrayList<>();
    serverStub = stub.executeTransaction(new StreamObserver<>() {
      @Override
      public void onNext(StreamingDatabaseResponse value) {
        for (KeyValue kv : value.getGetRange().getKeyValuesList()) {
          keyValues.add(kv.getKey().toStringUtf8());
        }
        if (value.getGetRange().getDone()) {
          latch.countDown();
        }
      }

      @Override
      public void onError(Throwable t) {
        fail(t.getMessage());
        latch.countDown();
      }

      @Override
      public void onCompleted() {
        latch.countDown();
      }
    });
    serverStub.onNext(StreamingDatabaseRequest.newBuilder().
        setStartTransaction(StartTransactionRequest.newBuilder().
            setName("setKeyAndCommit").
            setClientIdentifier("unit test").
            setDatabaseName("fdb").
            build()).
        build());
    serverStub.onNext(StreamingDatabaseRequest.newBuilder().
        setGetRange(GetRangeRequest.newBuilder().
            setStartKeySelector(equals("hello0".getBytes(StandardCharsets.UTF_8))).
            setEndKeySelector(equals("hello99999".getBytes(StandardCharsets.UTF_8))).
            setLimit(5000).
            setReverse(true).
            setStreamingMode(StreamingMode.ITERATOR).
            build()).
        build());

    latch.await(5, TimeUnit.SECONDS);
    serverStub.onCompleted();

    assertEquals(5000, keyValues.size());
    assertEquals("hello9999", keyValues.get(0));
    assertEquals("hello5499", keyValues.get(4999));
  }

  @Test
  public void testInvalidGetRangeWithExact() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);

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
            setSequenceId(12345).
            setStartBytes(ByteString.EMPTY).
            setEndBytes(ByteString.EMPTY).
            setStreamingMode(StreamingMode.EXACT).
            build()).
        build());

    verify(streamObs, timeout(5000).times(1)).onNext(streamingDatabaseResponseCapture.capture());
    serverStub.onCompleted();
    verify(streamObs, timeout(5000).times(1)).onCompleted();
    verify(streamObs, never()).onError(any());

    StreamingDatabaseResponse value = streamingDatabaseResponseCapture.getValue();
    assertTrue(value.hasOperationFailure());
    assertEquals(12345, value.getOperationFailure().getSequenceId());
  }

  private byte[] setupRangeTest(TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub) {
    clearRangeAndCommit(stub, "hello".getBytes(StandardCharsets.UTF_8),
        "hello4".getBytes(StandardCharsets.UTF_8));

    byte[] worldB = "world".getBytes(StandardCharsets.UTF_8);
    setKeyAndCommit(stub, "hello".getBytes(StandardCharsets.UTF_8), worldB);
    setKeyAndCommit(stub, "hello1".getBytes(StandardCharsets.UTF_8), worldB);
    setKeyAndCommit(stub, "hello2".getBytes(StandardCharsets.UTF_8), worldB);
    setKeyAndCommit(stub, "hello3".getBytes(StandardCharsets.UTF_8), worldB);
    return worldB;
  }

  private List<KeyValue> getRange(TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub,
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
    verify(streamObs, timeout(5000).times(1)).onCompleted();
    verify(streamObs, never()).onError(any());

    StreamingDatabaseResponse value = streamingDatabaseResponseCapture.getValue();
    assertTrue(value.hasGetRange());
    assertTrue(value.getGetRange().getDone());

    return value.getGetRange().getKeyValuesList();
  }

  private KeySelector keySelector(byte[] key, int offset, boolean orEqual) {
    return KeySelector.newBuilder().setKey(ByteString.copyFrom(key)).setOffset(offset).setOrEqual(orEqual).build();
  }

  private KeySelector equals(byte[] key) {
    return KeySelector.newBuilder().setKey(ByteString.copyFrom(key)).setOffset(1).setOrEqual(false).build();
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

  private long clearRangeAndCommit(TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub,
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
