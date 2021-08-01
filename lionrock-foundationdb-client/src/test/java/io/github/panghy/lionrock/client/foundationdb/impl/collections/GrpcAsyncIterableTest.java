package io.github.panghy.lionrock.client.foundationdb.impl.collections;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
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
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class GrpcAsyncIterableTest {

  @Captor
  ArgumentCaptor<Consumer<GetRangeResponse>> respCaptor;
  @Captor
  ArgumentCaptor<Consumer<OperationFailureResponse>> failureCaptor;

  @Test
  void iterator() {
    // setup mocks.
    GrpcAsyncIterator.RemovalCallback<KeyValue> removalCallback = mock(GrpcAsyncIterator.RemovalCallback.class);

    GrpcAsyncIterable.FetchIssuer<GetRangeResponse> fetchIssuer = mock(GrpcAsyncIterable.FetchIssuer.class);

    GrpcAsyncIterable<KeyValue, GetRangeResponse> iterable = new GrpcAsyncIterable<>(
        removalCallback, resp -> resp.getKeyValuesList().stream().map(x ->
        new com.apple.foundationdb.KeyValue(x.getKey().toByteArray(), x.getValue().toByteArray())),
        GetRangeResponse::getDone, fetchIssuer, MoreExecutors.directExecutor());
    AsyncIterator<KeyValue> iterator = iterable.iterator();

    verify(fetchIssuer, times(1)).issue(respCaptor.capture(), failureCaptor.capture());
    Consumer<GetRangeResponse> value = respCaptor.getValue();

    CompletableFuture<Boolean> cf = iterator.onHasNext();
    assertFalse(cf.isDone());
    value.accept(GetRangeResponse.newBuilder().
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
    value.accept(GetRangeResponse.newBuilder().
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

    verify(removalCallback, times(1)).deleteKey(eq(
        new KeyValue("hello2".getBytes(StandardCharsets.UTF_8),
            "world2".getBytes(StandardCharsets.UTF_8))));

    // get another iterator.
    iterator = iterable.iterator();
    verify(fetchIssuer, times(2)).issue(respCaptor.capture(), failureCaptor.capture());
    value = respCaptor.getValue();
    cf = iterator.onHasNext();
    assertFalse(cf.isDone());
    value.accept(GetRangeResponse.newBuilder().
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
    GrpcAsyncIterator.RemovalCallback<KeyValue> removalCallback = mock(GrpcAsyncIterator.RemovalCallback.class);

    GrpcAsyncIterable.FetchIssuer<GetRangeResponse> fetchIssuer = mock(GrpcAsyncIterable.FetchIssuer.class);

    GrpcAsyncIterable<KeyValue, GetRangeResponse> iterable = new GrpcAsyncIterable<KeyValue, GetRangeResponse>(
        removalCallback,
        resp -> resp.getKeyValuesList().stream().map(x ->
            new com.apple.foundationdb.KeyValue(x.getKey().toByteArray(), x.getValue().toByteArray())),
        GetRangeResponse::getDone,
        fetchIssuer,
        MoreExecutors.directExecutor());
    CompletableFuture<List<KeyValue>> cf = iterable.asList();
    assertFalse(cf.isDone());

    verify(fetchIssuer, times(1)).issue(respCaptor.capture(), failureCaptor.capture());
    Consumer<GetRangeResponse> value = respCaptor.getValue();

    value.accept(GetRangeResponse.newBuilder().
        addKeyValues(io.github.panghy.lionrock.proto.KeyValue.newBuilder().
            setKey(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom("world", StandardCharsets.UTF_8)).
            build()).
        build());
    assertFalse(cf.isDone());
    value.accept(GetRangeResponse.newBuilder().
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
    GrpcAsyncIterator.RemovalCallback<KeyValue> removalCallback = mock(GrpcAsyncIterator.RemovalCallback.class);

    GrpcAsyncIterable.FetchIssuer<GetRangeResponse> fetchIssuer = mock(GrpcAsyncIterable.FetchIssuer.class);

    GrpcAsyncIterable<KeyValue, GetRangeResponse> iterable = new GrpcAsyncIterable<>(
        removalCallback,
        resp -> resp.getKeyValuesList().stream().map(x ->
            new com.apple.foundationdb.KeyValue(x.getKey().toByteArray(), x.getValue().toByteArray())),
        GetRangeResponse::getDone,
        fetchIssuer,
        MoreExecutors.directExecutor());
    AsyncIterator<KeyValue> iterator = iterable.iterator();

    verify(fetchIssuer, times(1)).issue(respCaptor.capture(), failureCaptor.capture());
    Consumer<GetRangeResponse> value = respCaptor.getValue();

    CompletableFuture<Boolean> cf = iterator.onHasNext();
    assertFalse(cf.isDone());

    value.accept(GetRangeResponse.newBuilder().
        addKeyValues(io.github.panghy.lionrock.proto.KeyValue.newBuilder().
            setKey(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom("world", StandardCharsets.UTF_8)).
            build()).
        build());

    Consumer<OperationFailureResponse> failureResponseConsumer = failureCaptor.getValue();
    failureResponseConsumer.accept(
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
    GrpcAsyncIterator.RemovalCallback<KeyValue> removalCallback = mock(GrpcAsyncIterator.RemovalCallback.class);

    GrpcAsyncIterable.FetchIssuer<GetRangeResponse> fetchIssuer = mock(GrpcAsyncIterable.FetchIssuer.class);

    GrpcAsyncIterable<KeyValue, GetRangeResponse> iterable = new GrpcAsyncIterable<>(
        removalCallback,
        resp -> resp.getKeyValuesList().stream().map(x ->
            new com.apple.foundationdb.KeyValue(x.getKey().toByteArray(), x.getValue().toByteArray())),
        GetRangeResponse::getDone,
        fetchIssuer,
        MoreExecutors.directExecutor());

    CompletableFuture<List<KeyValue>> listCompletableFuture = iterable.asList();
    assertFalse(listCompletableFuture.isDone());

    verify(fetchIssuer, times(1)).issue(respCaptor.capture(), failureCaptor.capture());
    Consumer<GetRangeResponse> value = respCaptor.getValue();

    value.accept(GetRangeResponse.newBuilder().
        addKeyValues(io.github.panghy.lionrock.proto.KeyValue.newBuilder().
            setKey(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
            setValue(ByteString.copyFrom("world", StandardCharsets.UTF_8)).
            build()).
        build());
    assertFalse(listCompletableFuture.isDone());

    Consumer<OperationFailureResponse> failureResponseConsumer = failureCaptor.getValue();
    failureResponseConsumer.accept(
        OperationFailureResponse.newBuilder().setMessage("failed!!!").setCode(123).build());

    assertTrue(listCompletableFuture.isDone());
    assertTrue(listCompletableFuture.isCompletedExceptionally());
  }
}