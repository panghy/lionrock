package io.github.panghy.lionrock.client.foundationdb.impl.collections;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.github.panghy.lionrock.client.foundationdb.impl.StreamingDatabaseResponseVisitor;
import io.github.panghy.lionrock.proto.GetRangeResponse;
import io.github.panghy.lionrock.proto.OperationFailureResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class GrpcAsyncKeyValueIterableTest {

  @Captor
  ArgumentCaptor<StreamingDatabaseResponseVisitor> visitorCaptor;

  @Test
  void iterator() {
    // setup mocks.
    GrpcAsyncKeyValueIterator.RemovalCallback removalCallback = mock(GrpcAsyncKeyValueIterator.RemovalCallback.class);

    GrpcAsyncKeyValueIterable.GetRangeSupplier getRangeSupplier = mock(GrpcAsyncKeyValueIterable.GetRangeSupplier.class);
    Executor executor = mock(Executor.class);

    GrpcAsyncKeyValueIterable iterable = new GrpcAsyncKeyValueIterable(removalCallback, getRangeSupplier, executor);
    AsyncIterator<KeyValue> iterator = iterable.iterator();

    verify(getRangeSupplier, times(1)).issueGetRange(visitorCaptor.capture());
    StreamingDatabaseResponseVisitor value = visitorCaptor.getValue();

    CompletableFuture<Boolean> cf = iterator.onHasNext();
    assertFalse(cf.isDone());
    value.handleGetRange(GetRangeResponse.newBuilder().
        addKeyValues(io.github.panghy.lionrock.proto.KeyValue.newBuilder().
            setKey(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom("world", StandardCharsets.UTF_8)).
            build()).
        build());
    assertTrue(cf.isDone());
    assertTrue(iterator.hasNext());
    KeyValue next = iterator.next();
    assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), next.getKey());

    cf = iterator.onHasNext();
    assertFalse(cf.isDone());
    value.handleGetRange(GetRangeResponse.newBuilder().
        setDone(true).
        addKeyValues(io.github.panghy.lionrock.proto.KeyValue.newBuilder().
            setKey(ByteString.copyFrom("hello2", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom("world2", StandardCharsets.UTF_8)).
            build()).
        build());
    assertTrue(cf.isDone());
    assertTrue(iterator.hasNext());
    next = iterator.next();
    assertArrayEquals("hello2".getBytes(StandardCharsets.UTF_8), next.getKey());
    iterator.remove();
    iterator.onHasNext();
    assertTrue(cf.isDone());
    assertFalse(iterator.hasNext());

    verify(removalCallback, times(1)).deleteKey(eq("hello2".getBytes(StandardCharsets.UTF_8)));

    // get another iterator.
    iterator = iterable.iterator();
    verify(getRangeSupplier, times(2)).issueGetRange(visitorCaptor.capture());
    value = visitorCaptor.getValue();
    cf = iterator.onHasNext();
    assertFalse(cf.isDone());
    value.handleGetRange(GetRangeResponse.newBuilder().
        setDone(true).
        addKeyValues(io.github.panghy.lionrock.proto.KeyValue.newBuilder().
            setKey(ByteString.copyFrom("hello2", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom("world2", StandardCharsets.UTF_8)).
            build()).
        build());
    assertTrue(cf.isDone());
    assertTrue(iterator.hasNext());
    next = iterator.next();
    assertArrayEquals("hello2".getBytes(StandardCharsets.UTF_8), next.getKey());
    iterator.onHasNext();
    assertTrue(cf.isDone());
    assertFalse(iterator.hasNext());
  }

  @Test
  void asList() {
    // setup mocks.
    GrpcAsyncKeyValueIterator.RemovalCallback removalCallback = mock(GrpcAsyncKeyValueIterator.RemovalCallback.class);

    GrpcAsyncKeyValueIterable.GetRangeSupplier getRangeSupplier = mock(GrpcAsyncKeyValueIterable.GetRangeSupplier.class);

    GrpcAsyncKeyValueIterable iterable = new GrpcAsyncKeyValueIterable(removalCallback, getRangeSupplier,
        MoreExecutors.directExecutor());
    CompletableFuture<List<KeyValue>> cf = iterable.asList();
    assertFalse(cf.isDone());

    verify(getRangeSupplier, times(1)).issueGetRange(visitorCaptor.capture());
    StreamingDatabaseResponseVisitor value = visitorCaptor.getValue();

    value.handleGetRange(GetRangeResponse.newBuilder().
        addKeyValues(io.github.panghy.lionrock.proto.KeyValue.newBuilder().
            setKey(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom("world", StandardCharsets.UTF_8)).
            build()).
        build());
    assertFalse(cf.isDone());
    value.handleGetRange(GetRangeResponse.newBuilder().
        setDone(true).
        addKeyValues(io.github.panghy.lionrock.proto.KeyValue.newBuilder().
            setKey(ByteString.copyFrom("hello2", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom("world2", StandardCharsets.UTF_8)).
            build()).
        build());
    assertTrue(cf.isDone());

    List<KeyValue> keyValues = cf.join();
    assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), keyValues.get(0).getKey());
    assertArrayEquals("hello2".getBytes(StandardCharsets.UTF_8), keyValues.get(1).getKey());
  }

  @Test
  void iterator_testFailure() {
    // setup mocks.
    GrpcAsyncKeyValueIterator.RemovalCallback removalCallback = mock(GrpcAsyncKeyValueIterator.RemovalCallback.class);
    GrpcAsyncKeyValueIterable.GetRangeSupplier getRangeSupplier =
        mock(GrpcAsyncKeyValueIterable.GetRangeSupplier.class);

    GrpcAsyncKeyValueIterable iterable = new GrpcAsyncKeyValueIterable(removalCallback, getRangeSupplier,
        MoreExecutors.directExecutor());
    AsyncIterator<KeyValue> iterator = iterable.iterator();

    verify(getRangeSupplier, times(1)).issueGetRange(visitorCaptor.capture());
    StreamingDatabaseResponseVisitor value = visitorCaptor.getValue();

    CompletableFuture<Boolean> cf = iterator.onHasNext();
    assertFalse(cf.isDone());

    value.handleGetRange(GetRangeResponse.newBuilder().
        addKeyValues(io.github.panghy.lionrock.proto.KeyValue.newBuilder().
            setKey(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom("world", StandardCharsets.UTF_8)).
            build()).
        build());
    value.handleOperationFailure(
        OperationFailureResponse.newBuilder().setMessage("failed!!!").setCode(123).build());

    assertTrue(cf.isDone());
    assertTrue(iterator.hasNext());
    KeyValue next = iterator.next();
    assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), next.getKey());

    cf = iterator.onHasNext();
    assertTrue(cf.isCompletedExceptionally());
  }

  @Test
  void asList_testFailure() {
    // setup mocks.
    GrpcAsyncKeyValueIterator.RemovalCallback removalCallback = mock(GrpcAsyncKeyValueIterator.RemovalCallback.class);
    GrpcAsyncKeyValueIterable.GetRangeSupplier getRangeSupplier =
        mock(GrpcAsyncKeyValueIterable.GetRangeSupplier.class);

    GrpcAsyncKeyValueIterable iterable = new GrpcAsyncKeyValueIterable(removalCallback, getRangeSupplier,
        MoreExecutors.directExecutor());

    CompletableFuture<List<KeyValue>> listCompletableFuture = iterable.asList();
    assertFalse(listCompletableFuture.isDone());

    verify(getRangeSupplier, times(1)).issueGetRange(visitorCaptor.capture());
    StreamingDatabaseResponseVisitor value = visitorCaptor.getValue();

    value.handleGetRange(GetRangeResponse.newBuilder().
        addKeyValues(io.github.panghy.lionrock.proto.KeyValue.newBuilder().
            setKey(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom("world", StandardCharsets.UTF_8)).
            build()).
        build());
    assertFalse(listCompletableFuture.isDone());

    value.handleOperationFailure(
        OperationFailureResponse.newBuilder().setMessage("failed!!!").setCode(123).build());
    assertTrue(listCompletableFuture.isDone());
    assertTrue(listCompletableFuture.isCompletedExceptionally());
  }
}