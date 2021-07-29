package io.github.panghy.lionrock.foundationdb;

import com.google.protobuf.ByteString;
import io.github.panghy.lionrock.proto.*;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lognet.springboot.grpc.autoconfigure.GRpcServerProperties;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class UnaryOperationTests extends AbstractGrpcTest {

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
  public void testInvalidRangeGetWithExact() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);
    StreamObserver<DatabaseResponse> streamObs = mock(StreamObserver.class);

    stub.execute(DatabaseRequest.newBuilder().
        setDatabaseName("fdb").setName("testGetRange").setClientIdentifier("unit test").
        setGetRange(GetRangeRequest.newBuilder().
            setStartBytes(ByteString.EMPTY).
            setEndBytes(ByteString.EMPTY).
            setStreamingMode(StreamingMode.EXACT).
            build()).
        build(), streamObs);

    verify(streamObs, never()).onNext(databaseResponseCapture.capture());
    verify(streamObs, never()).onCompleted();
    verify(streamObs, timeout(5000).times(1)).onError(any());
  }

  @Test
  public void testGetKey_keyselectorStart_firstGreaterThan() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);

    setupRangeTest(stub);

    // firstAfter hello
    byte[] result = getKey(stub, keySelector("hello".getBytes(StandardCharsets.UTF_8), 1, true));
    assertArrayEquals("hello1".getBytes(StandardCharsets.UTF_8), result);
  }

  @Test
  public void testGetKey_keyselectorStart_firstGreaterThanOrEqual() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);
    setupRangeTest(stub);

    // firstAfterOrEqual to Hello
    byte[] result = getKey(stub, keySelector("hello".getBytes(StandardCharsets.UTF_8), 1, false));
    assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), result);
  }

  @Test
  public void testGetKey_keyselectorStart_lastLessThanOrEqual() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);
    setupRangeTest(stub);

    // lastLessThanOrEqual "hello"
    byte[] result = getKey(stub, keySelector("hello".getBytes(StandardCharsets.UTF_8), 0, true));
    assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), result);
    // lastLessThanOrEqual hello0 which is hello.
    result = getKey(stub, keySelector("hello0".getBytes(StandardCharsets.UTF_8), 0, true));
    assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), result);
  }

  @Test
  public void testGetKey_keyselectorStart_lastLessThan() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);
    setupRangeTest(stub);

    // lastLessThan hello0 which is hello.
    byte[] result = getKey(stub, keySelector("hello0".getBytes(StandardCharsets.UTF_8), 0, false));
    assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), result);
    // lastLessThan hello1 which is hello.
    result = getKey(stub, keySelector("hello1".getBytes(StandardCharsets.UTF_8), 0, false));
    assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), result);
  }

  @Test
  public void testGetEstimateRangeSize() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);
    setupRangeTest(stub);

    StreamObserver<DatabaseResponse> streamObs = mock(StreamObserver.class);
    stub.execute(DatabaseRequest.newBuilder().
        setDatabaseName("fdb").setName("getEstimatedRangeSize").setClientIdentifier("unit test").
        setGetEstimatedRangeSize(GetEstimatedRangeSizeRequest.newBuilder().
            setStart(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
            setEnd(ByteString.copyFrom("hello4", StandardCharsets.UTF_8)).
            build()).
        build(), streamObs);

    verify(streamObs, timeout(5000).times(1)).onNext(databaseResponseCapture.capture());
    verify(streamObs, timeout(5000).times(1)).onCompleted();
    verify(streamObs, never()).onError(any());

    DatabaseResponse value = databaseResponseCapture.getValue();
    assertTrue(value.hasGetEstimatedRangeSize());
    // we can't assert the result.
    // assertTrue(value.getGetEstimatedRangeSize().getSize() > 0);
  }

  @Test
  public void testGetBoundaryKeys() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);
    setupRangeTest(stub);

    StreamObserver<DatabaseResponse> streamObs = mock(StreamObserver.class);
    stub.execute(DatabaseRequest.newBuilder().
        setDatabaseName("fdb").setName("getBoundaryKeys").setClientIdentifier("unit test").
        setGetBoundaryKeys(GetBoundaryKeysRequest.newBuilder().
            setStart(ByteString.copyFrom(new byte[]{0})).
            setEnd(ByteString.copyFrom(new byte[]{-1})).
            build()).
        build(), streamObs);

    verify(streamObs, timeout(5000).times(1)).onNext(databaseResponseCapture.capture());
    verify(streamObs, timeout(5000).times(1)).onCompleted();
    verify(streamObs, never()).onError(any());

    DatabaseResponse value = databaseResponseCapture.getValue();
    assertTrue(value.hasGetBoundaryKeys());
    assertTrue(value.getGetBoundaryKeys().getKeysCount() > 0);
  }

  @Test
  public void testGetAddressesForKey() {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        TransactionalKeyValueStoreGrpc.newStub(channel);
    setupRangeTest(stub);

    StreamObserver<DatabaseResponse> streamObs = mock(StreamObserver.class);
    stub.execute(DatabaseRequest.newBuilder().
        setDatabaseName("fdb").setName("getAddressesForKey").setClientIdentifier("unit test").
        setGetAddressesForKey(GetAddressesForKeyRequest.newBuilder().
            setKey(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
            build()).
        build(), streamObs);

    verify(streamObs, timeout(5000).times(1)).onNext(databaseResponseCapture.capture());
    verify(streamObs, timeout(5000).times(1)).onCompleted();
    verify(streamObs, never()).onError(any());

    DatabaseResponse value = databaseResponseCapture.getValue();
    assertTrue(value.hasGetAddresssesForKey());
    assertTrue(value.getGetAddresssesForKey().getAddressesCount() > 0);
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

  private byte[] getKey(TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub, KeySelector keySelector) {
    StreamObserver<DatabaseResponse> streamObs = mock(StreamObserver.class);
    stub.execute(DatabaseRequest.newBuilder().
        setDatabaseName("fdb").setName("getKey").setClientIdentifier("unit test").
        setGetKey(GetKeyRequest.newBuilder().setKeySelector(keySelector).build()).
        build(), streamObs);

    verify(streamObs, timeout(5000).times(1)).onNext(databaseResponseCapture.capture());
    verify(streamObs, timeout(5000).times(1)).onCompleted();
    verify(streamObs, never()).onError(any());

    DatabaseResponse value = databaseResponseCapture.getValue();
    assertTrue(value.hasGetKey());
    return value.getGetKey().hasKey() ? value.getGetKey().getKey().toByteArray() : null;
  }

  private List<KeyValue> getRange(TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub,
                                  KeySelector start, KeySelector end, int limit, boolean reverse) {
    StreamObserver<DatabaseResponse> streamObs = mock(StreamObserver.class);
    stub.execute(DatabaseRequest.newBuilder().
        setDatabaseName("fdb").setName("testGetRange").setClientIdentifier("unit test").
        setGetRange(GetRangeRequest.newBuilder().
            setStartKeySelector(start).
            setEndKeySelector(end).
            setLimit(limit).
            setReverse(reverse).build()).
        build(), streamObs);

    verify(streamObs, timeout(5000).times(1)).onNext(databaseResponseCapture.capture());
    verify(streamObs, timeout(5000).times(1)).onCompleted();
    verify(streamObs, never()).onError(any());

    DatabaseResponse value = databaseResponseCapture.getValue();
    assertTrue(value.hasGetRange());
    return value.getGetRange().getKeyValuesList();
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
