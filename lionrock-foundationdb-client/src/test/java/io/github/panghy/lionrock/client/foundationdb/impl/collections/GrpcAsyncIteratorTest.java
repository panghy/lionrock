package io.github.panghy.lionrock.client.foundationdb.impl.collections;

import com.apple.foundationdb.FDBException;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.github.panghy.lionrock.proto.GetRangeResponse;
import io.github.panghy.lionrock.proto.KeyValue;
import io.github.panghy.lionrock.proto.OperationFailureResponse;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class GrpcAsyncIteratorTest {

  @Test
  public void testIterator() {
    GrpcAsyncIterator.RemovalCallback<com.apple.foundationdb.KeyValue> removalCallback =
        mock(GrpcAsyncIterator.RemovalCallback.class);
    GrpcAsyncIterator<com.apple.foundationdb.KeyValue, GetRangeResponse> iterator =
        new GrpcAsyncIterator<>(removalCallback,
            resp -> resp.getKeyValuesList().stream().map(x ->
                new com.apple.foundationdb.KeyValue(x.getKey().toByteArray(), x.getValue().toByteArray())),
            GetRangeResponse::getDone, MoreExecutors.directExecutor());

    // add 1st and 2nd element and dequeue (also check remove()).
    CompletableFuture<Boolean> cf = iterator.onHasNext();
    assertFalse(cf.isDone());
    iterator.accept(GetRangeResponse.newBuilder().
        addKeyValues(
            KeyValue.newBuilder().
                setKey(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
                setValue(ByteString.copyFrom("world", StandardCharsets.UTF_8)).build()).
        addKeyValues(
            KeyValue.newBuilder().
                setKey(ByteString.copyFrom("hello2", StandardCharsets.UTF_8)).
                setValue(ByteString.copyFrom("world2", StandardCharsets.UTF_8)).build()).
        build());
    assertTrue(cf.isDone());
    assertTrue(iterator.hasNext());
    com.apple.foundationdb.KeyValue next = iterator.next();
    assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), next.getKey());
    cf = iterator.onHasNext();
    assertTrue(cf.isDone());
    assertTrue(iterator.hasNext());
    next = iterator.next();
    assertArrayEquals("hello2".getBytes(StandardCharsets.UTF_8), next.getKey());
    iterator.remove();
    verify(removalCallback, times(1)).deleteKey(eq(new com.apple.foundationdb.KeyValue(
        "hello2".getBytes(StandardCharsets.UTF_8), "world2".getBytes(StandardCharsets.UTF_8))));

    // add 3rd and 4th element element as separate RPC calls before dequeueing.
    cf = iterator.onHasNext();
    assertFalse(cf.isDone());
    cf = iterator.onHasNext();
    assertFalse(cf.isDone());
    iterator.accept(GetRangeResponse.newBuilder().
        addKeyValues(
            KeyValue.newBuilder().
                setKey(ByteString.copyFrom("hello3", StandardCharsets.UTF_8)).
                setValue(ByteString.copyFrom("world3", StandardCharsets.UTF_8)).build()).
        build());
    iterator.accept(GetRangeResponse.newBuilder().
        addKeyValues(
            KeyValue.newBuilder().
                setKey(ByteString.copyFrom("hello4", StandardCharsets.UTF_8)).
                setValue(ByteString.copyFrom("world4", StandardCharsets.UTF_8)).build()).
        build());
    assertTrue(cf.isDone());
    assertTrue(iterator.hasNext());
    next = iterator.next();
    assertArrayEquals("hello3".getBytes(StandardCharsets.UTF_8), next.getKey());
    cf = iterator.onHasNext();
    assertTrue(cf.isDone());
    assertTrue(iterator.hasNext());

    // add 5th element before dequeueing.
    iterator.accept(GetRangeResponse.newBuilder().
        addKeyValues(
            KeyValue.newBuilder().
                setKey(ByteString.copyFrom("hello5", StandardCharsets.UTF_8)).
                setValue(ByteString.copyFrom("world5", StandardCharsets.UTF_8)).build()).
        build());
    next = iterator.next();
    assertArrayEquals("hello4".getBytes(StandardCharsets.UTF_8), next.getKey());
    cf = iterator.onHasNext();
    assertTrue(cf.isDone());
    next = iterator.next();
    assertArrayEquals("hello5".getBytes(StandardCharsets.UTF_8), next.getKey());

    // add 6th element before dequeueing and mark done.
    cf = iterator.onHasNext();
    assertFalse(cf.isDone());
    iterator.accept(GetRangeResponse.newBuilder().
        setDone(true).
        addKeyValues(
            KeyValue.newBuilder().
                setKey(ByteString.copyFrom("hello6", StandardCharsets.UTF_8)).
                setValue(ByteString.copyFrom("world6", StandardCharsets.UTF_8)).build()).
        build());
    next = iterator.next();
    assertArrayEquals("hello6".getBytes(StandardCharsets.UTF_8), next.getKey());
    cf = iterator.onHasNext();
    assertTrue(cf.isDone());
    assertFalse(cf.join());
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testIterator_failure() {
    GrpcAsyncIterator.RemovalCallback<com.apple.foundationdb.KeyValue> removalCallback =
        mock(GrpcAsyncIterator.RemovalCallback.class);
    GrpcAsyncIterator<com.apple.foundationdb.KeyValue, GetRangeResponse> iterator =
        new GrpcAsyncIterator<>(removalCallback,
            resp -> resp.getKeyValuesList().stream().map(x ->
                new com.apple.foundationdb.KeyValue(x.getKey().toByteArray(), x.getValue().toByteArray())),
            GetRangeResponse::getDone, MoreExecutors.directExecutor());

    // add 1st and 2nd element.
    CompletableFuture<Boolean> cf = iterator.onHasNext();
    assertFalse(cf.isDone());
    iterator.accept(GetRangeResponse.newBuilder().
        addKeyValues(
            KeyValue.newBuilder().
                setKey(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
                setValue(ByteString.copyFrom("world", StandardCharsets.UTF_8)).build()).
        addKeyValues(
            KeyValue.newBuilder().
                setKey(ByteString.copyFrom("hello2", StandardCharsets.UTF_8)).
                setValue(ByteString.copyFrom("world2", StandardCharsets.UTF_8)).build()).
        build());
    assertTrue(cf.isDone());
    assertTrue(iterator.hasNext());
    com.apple.foundationdb.KeyValue next = iterator.next();
    assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), next.getKey());
    cf = iterator.onHasNext();
    assertTrue(cf.isDone());
    assertTrue(iterator.hasNext());
    next = iterator.next();
    assertArrayEquals("hello2".getBytes(StandardCharsets.UTF_8), next.getKey());

    cf = iterator.onHasNext();
    assertFalse(cf.isDone());

    iterator.accept(OperationFailureResponse.newBuilder().setCode(123).setMessage("failed").build());
    assertTrue(cf.isCompletedExceptionally());
    FDBException join = (FDBException) cf.handle((aBoolean, throwable) -> throwable).join();
    assertEquals(123, join.getCode());
    assertEquals("failed", join.getMessage());

    // does not change the state.
    iterator.accept(GetRangeResponse.newBuilder().
        addKeyValues(
            KeyValue.newBuilder().
                setKey(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
                setValue(ByteString.copyFrom("world", StandardCharsets.UTF_8)).build()).
        addKeyValues(
            KeyValue.newBuilder().
                setKey(ByteString.copyFrom("hello2", StandardCharsets.UTF_8)).
                setValue(ByteString.copyFrom("world2", StandardCharsets.UTF_8)).build()).
        build());

    cf = iterator.onHasNext();
    assertTrue(cf.isCompletedExceptionally());
  }

  @Test
  public void testIterator_emptyResponse() {
    GrpcAsyncIterator.RemovalCallback<com.apple.foundationdb.KeyValue> removalCallback =
        mock(GrpcAsyncIterator.RemovalCallback.class);
    GrpcAsyncIterator<com.apple.foundationdb.KeyValue, GetRangeResponse> iterator =
        new GrpcAsyncIterator<>(removalCallback,
            resp -> resp.getKeyValuesList().stream().map(x ->
                new com.apple.foundationdb.KeyValue(x.getKey().toByteArray(), x.getValue().toByteArray())),
            GetRangeResponse::getDone, MoreExecutors.directExecutor());

    // add 1st and 2nd element and dequeue (also check remove()).
    CompletableFuture<Boolean> cf = iterator.onHasNext();
    assertFalse(cf.isDone());

    iterator.accept(GetRangeResponse.newBuilder().
        setDone(true).
        build());
    assertTrue(cf.isDone());
    assertTrue(cf.isDone());
    assertFalse(cf.join());

    cf = iterator.onHasNext();
    assertTrue(cf.isDone());
    assertFalse(cf.join());
    assertFalse(iterator.hasNext());
    try {
      iterator.next();
      fail();
    } catch (NoSuchElementException expected) {
    }
  }

  @Test
  public void testIterator_failed() {
    GrpcAsyncIterator.RemovalCallback<com.apple.foundationdb.KeyValue> removalCallback =
        mock(GrpcAsyncIterator.RemovalCallback.class);
    GrpcAsyncIterator<com.apple.foundationdb.KeyValue, GetRangeResponse> iterator =
        new GrpcAsyncIterator<>(removalCallback,
            resp -> resp.getKeyValuesList().stream().map(x ->
                new com.apple.foundationdb.KeyValue(x.getKey().toByteArray(), x.getValue().toByteArray())),
            GetRangeResponse::getDone, MoreExecutors.directExecutor());

    // add 1st and 2nd element and dequeue (also check remove()).
    CompletableFuture<Boolean> cf = iterator.onHasNext();
    assertFalse(cf.isDone());

    iterator.accept(GetRangeResponse.newBuilder().
        addKeyValues(
            KeyValue.newBuilder().
                setKey(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).
                setValue(ByteString.copyFrom("world", StandardCharsets.UTF_8)).build()).
        addKeyValues(
            KeyValue.newBuilder().
                setKey(ByteString.copyFrom("hello2", StandardCharsets.UTF_8)).
                setValue(ByteString.copyFrom("world2", StandardCharsets.UTF_8)).build()).
        build());
    assertTrue(cf.isDone());
    iterator.next();
    iterator.next();

    cf = iterator.onHasNext();

    iterator.accept(OperationFailureResponse.newBuilder().setCode(123).setMessage("failed").build());
    assertTrue(cf.isDone());
    assertTrue(cf.isCompletedExceptionally());
    // ignored.
    iterator.accept(GetRangeResponse.newBuilder().setDone(true).build());

    cf = iterator.onHasNext();
    assertTrue(cf.isDone());
    assertTrue(cf.isCompletedExceptionally());
    try {
      iterator.next();
      fail();
    } catch (Exception expected) {
    }
  }

  @Test()
  public void testIterator_cancel() {
    GrpcAsyncIterator.RemovalCallback<com.apple.foundationdb.KeyValue> removalCallback =
        mock(GrpcAsyncIterator.RemovalCallback.class);
    GrpcAsyncIterator<com.apple.foundationdb.KeyValue, GetRangeResponse> iterator =
        new GrpcAsyncIterator<>(removalCallback,
            resp -> resp.getKeyValuesList().stream().map(x ->
                new com.apple.foundationdb.KeyValue(x.getKey().toByteArray(), x.getValue().toByteArray())),
            GetRangeResponse::getDone, MoreExecutors.directExecutor());
    try {
      // can't remove before having first fetch.
      iterator.remove();
      fail();
    } catch (IllegalStateException expected) {
    }
    iterator.cancel();
    try {
      iterator.onHasNext();
      fail();
    } catch (IllegalStateException expected) {
    }
    try {
      iterator.hasNext();
      fail();
    } catch (IllegalStateException expected) {
    }
    try {
      iterator.next();
      fail();
    } catch (IllegalStateException expected) {
    }
  }
}