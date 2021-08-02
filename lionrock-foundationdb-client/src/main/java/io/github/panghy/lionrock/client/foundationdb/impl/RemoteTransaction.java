package io.github.panghy.lionrock.client.foundationdb.impl;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterable;
import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;
import io.github.panghy.lionrock.client.foundationdb.impl.collections.GrpcAsyncIterable;
import io.github.panghy.lionrock.client.foundationdb.mixins.ReadTransactionMixin;
import io.github.panghy.lionrock.client.foundationdb.mixins.TransactionMixin;
import io.github.panghy.lionrock.proto.*;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.apple.foundationdb.tuple.ByteArrayUtil.printable;
import static java.util.concurrent.CompletableFuture.failedFuture;

/**
 * A {@link Transaction} that's backed by a gRPC streaming call.
 *
 * @author Clement Pang
 */
public class RemoteTransaction implements TransactionMixin {

  /**
   * Flush mutations when we have 1MB of them.
   */
  public static final int MUTATIONS_FLUSH_BATCH_THRESHOLD = 1_000_000;

  /**
   * Exception we set for the getVersionstamp() future when no write conflict ranges or mutations are present.
   */
  private static final FDBException NO_COMMIT_VERSION =
      new FDBException("Transaction is read-only and therefore does not have a commit version", 2021);

  private static final Metadata.Key<String> FDB_ERROR_CODE_KEY =
      Metadata.Key.of("fdb_error_code", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> FDB_ERROR_MESSAGE_KEY =
      Metadata.Key.of("fdb_error_message", Metadata.ASCII_STRING_MARSHALLER);

  /**
   * Used to send requests to the server.
   */
  private final StreamObserver<StreamingDatabaseRequest> requestSink;
  private final AtomicBoolean commitStarted = new AtomicBoolean(false);
  private final AtomicBoolean cancelled = new AtomicBoolean(false);
  private final AtomicBoolean completed = new AtomicBoolean(false);
  private final AtomicReference<CommitTransactionResponse> commitResponse = new AtomicReference<>();
  private final SequenceResponseDemuxer demuxer;
  private final RemoteTransactionContext remoteTransactionContext;
  private final MutationsBatcher batcher;
  private final MutationsBatcher conflictRangeBatcher;
  /**
   * Instance of {@link TransactionOptions} for this transaction context.
   */
  private final TransactionOptions transactionOptions = new TransactionOptions(new OptionConsumer() {
    @Override
    public void setOption(int code, byte[] parameter) {
      if (code == 500) {
        // timeout.
        long timeoutMs = ByteBuffer.wrap(parameter).order(ByteOrder.LITTLE_ENDIAN).getLong();
        remoteTransactionContext.setTimeout(timeoutMs);
        // we'll set the time-left on the server transaction too.
        long millisecondsLeft = remoteTransactionContext.getMillisecondsLeft();
        if (millisecondsLeft <= 0) {
          millisecondsLeft = 1;
        }
        SetTransactionOptionRequest.Builder builder = SetTransactionOptionRequest.newBuilder().
            setOption(code);
        ByteBuffer b = ByteBuffer.allocate(8);
        b.order(ByteOrder.LITTLE_ENDIAN);
        b.putLong(millisecondsLeft);
        builder.setParam(ByteString.copyFrom(b.array()));
        synchronized (requestSink) {
          requestSink.onNext(StreamingDatabaseRequest.newBuilder().
              setSetTransactionOption(builder.build()).build());
        }
      } else if (code == 501) {
        long retryLimit = ByteBuffer.wrap(parameter).order(ByteOrder.LITTLE_ENDIAN).getLong();
        remoteTransactionContext.setRetryLimit(retryLimit);
      } else if (code == 502) {
        long retryDelayMs = ByteBuffer.wrap(parameter).order(ByteOrder.LITTLE_ENDIAN).getLong();
        remoteTransactionContext.setMaxRetryDelay(retryDelayMs);
      } else {
        SetTransactionOptionRequest.Builder builder = SetTransactionOptionRequest.newBuilder().
            setOption(code);
        if (parameter != null) {
          builder.setParam(ByteString.copyFrom(parameter));
        }
        synchronized (requestSink) {
          requestSink.onNext(StreamingDatabaseRequest.newBuilder().
              setSetTransactionOption(builder.build()).build());
        }
      }
    }
  });
  /**
   * Instance of {@link ReadTransaction} that allows for snapshot reads.
   */
  private final RemoteReadTransaction readTransaction = new RemoteReadTransaction();
  private final Collection<CompletableFuture<?>> futures = new ArrayList<>();
  private final CompletableFuture<Void> commitFuture = new CompletableFuture<>();
  private final CompletableFuture<byte[]> versionStampFuture = newCompletableFuture("getVersionstamp");
  /**
   * Whether this is a read-only transaction. Versionstamp fetches and commit version can be optimized.
   */
  private final AtomicBoolean readOnlyTx = new AtomicBoolean(true);

  private volatile Throwable remoteError;

  public RemoteTransaction(RemoteTransactionContext remoteTransactionContext, long timeoutMs) {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub =
        remoteTransactionContext.getStub().withExecutor(remoteTransactionContext.getExecutor());
    if (timeoutMs > 0) {
      stub = stub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS);
    }
    remoteTransactionContext.incrementAttempts();
    // We can have nested transactions on the client, ensure that we don't use the smae context.
    Context newContext = Context.current().fork();
    Context origContext = newContext.attach();
    try {
      this.requestSink = stub.executeTransaction(new StreamObserver<>() {
        @Override
        public void onNext(StreamingDatabaseResponse value) {
          if (value.hasCommitTransaction()) {
            commitResponse.set(value.getCommitTransaction());
            if (value.getCommitTransaction().hasVersionstamp()) {
              versionStampFuture.complete(value.getCommitTransaction().getVersionstamp().toByteArray());
            } else if (!versionStampFuture.isDone()) {
              versionStampFuture.completeExceptionally(new FDBException("no_versionstamp_from_server", 4100));
            }
            close();
            commitFuture.complete(null);
          } else {
            demuxer.accept(value);
          }
        }

        @Override
        public void onError(Throwable t) {
          // if the server error-ed out, fail all outstanding future.
          Throwable unwrapped = unwrapFdbException(t);
          synchronized (futures) {
            futures.forEach(x -> x.completeExceptionally(unwrapped));
          }
          commitFuture.completeExceptionally(unwrapped);
          remoteError = unwrapped;
        }

        @Override
        public void onCompleted() {
          synchronized (futures) {
            // internal error if any future is still outstanding but the server left.
            futures.forEach(x -> {
              if (!x.isDone()) {
                x.completeExceptionally(new FDBException("server_left, dangling future: " + x, 4100));
              }
            });
          }
          completed.set(true);
        }
      });
    } finally {
      newContext.detach(origContext);
    }
    // start the transaction with the server.
    synchronized (requestSink) {
      this.requestSink.onNext(StreamingDatabaseRequest.newBuilder().
          setStartTransaction(StartTransactionRequest.newBuilder().
              setClientIdentifier(remoteTransactionContext.getDatabase().getClientIdentifier()).
              setDatabaseName(remoteTransactionContext.getDatabase().getDatabaseName()).
              setName(remoteTransactionContext.getName()).
              build()).build());
      // set the timeout on the transaction if we have one.
      long millisecondsLeft = remoteTransactionContext.getMillisecondsLeft();
      if (millisecondsLeft != Long.MAX_VALUE) {
        if (millisecondsLeft <= 0) millisecondsLeft = 1;
        SetTransactionOptionRequest.Builder builder = SetTransactionOptionRequest.newBuilder().
            setOption(500);
        ByteBuffer b = ByteBuffer.allocate(8);
        b.order(ByteOrder.LITTLE_ENDIAN);
        b.putLong(millisecondsLeft);
        builder.setParam(ByteString.copyFrom(b.array()));
        synchronized (requestSink) {
          requestSink.onNext(StreamingDatabaseRequest.newBuilder().
              setSetTransactionOption(builder.build()).build());
        }
      }
    }
    this.remoteTransactionContext = remoteTransactionContext;
    this.demuxer = new SequenceResponseDemuxer();
    this.batcher = new MutationsBatcher(requestSink, MUTATIONS_FLUSH_BATCH_THRESHOLD);
    this.conflictRangeBatcher = new MutationsBatcher(requestSink, MUTATIONS_FLUSH_BATCH_THRESHOLD);
  }

  private class RemoteReadTransaction implements ReadTransactionMixin {

    @Override
    public boolean isSnapshot() {
      return true;
    }

    @Override
    public ReadTransaction snapshot() {
      return this;
    }

    @Override
    public CompletableFuture<Long> getReadVersion() {
      return RemoteTransaction.this.getReadVersion();
    }

    @Override
    public void setReadVersion(long version) {
      RemoteTransaction.this.setReadVersion(version);
    }

    @Override
    public boolean addReadConflictRangeIfNotSnapshot(byte[] keyBegin, byte[] keyEnd) {
      // This is a snapshot transaction; do not add the conflict range.
      return false;
    }

    @Override
    public boolean addReadConflictKeyIfNotSnapshot(byte[] key) {
      // This is a snapshot transaction; do not add the conflict range.
      return false;
    }

    @Override
    public CompletableFuture<byte[]> get(byte[] key) {
      return RemoteTransaction.this.get(key, true);
    }

    @Override
    public CompletableFuture<byte[]> getKey(KeySelector selector) {
      return RemoteTransaction.this.getKey(selector, true);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, int limit, boolean reverse,
                                            StreamingMode mode) {
      return RemoteTransaction.this.getRange(begin, end, limit, reverse, mode, true);
    }

    @Override
    public CompletableFuture<Long> getEstimatedRangeSizeBytes(byte[] begin, byte[] end) {
      return RemoteTransaction.this.getEstimatedRangeSizeBytes(begin, end);
    }

    @Override
    public CompletableFuture<Long> getEstimatedRangeSizeBytes(Range range) {
      return RemoteTransaction.this.getEstimatedRangeSizeBytes(range);
    }

    @Override
    public TransactionOptions options() {
      return RemoteTransaction.this.options();
    }

    @Override
    public Executor getExecutor() {
      return RemoteTransaction.this.getExecutor();
    }
  }

  @Override
  public void addReadConflictKey(byte[] key) {
    conflictRangeBatcher.addConflictKey(
        AddConflictKeyRequest.newBuilder().
            setKey(ByteString.copyFrom(key)).
            setWrite(false).
            build());
  }

  @Override
  public void addWriteConflictKey(byte[] key) {
    conflictRangeBatcher.addConflictKey(
        AddConflictKeyRequest.newBuilder().
            setKey(ByteString.copyFrom(key)).
            setWrite(true).
            build());
  }

  @Override
  public void addReadConflictRange(byte[] keyBegin, byte[] keyEnd) {
    conflictRangeBatcher.addConflictRange(
        AddConflictRangeRequest.newBuilder().
            setStart(ByteString.copyFrom(keyBegin)).
            setEnd(ByteString.copyFrom(keyEnd)).
            setWrite(false).
            build());
  }

  @Override
  public void addWriteConflictRange(byte[] keyBegin, byte[] keyEnd) {
    readOnlyTx.set(false);
    conflictRangeBatcher.addConflictRange(
        AddConflictRangeRequest.newBuilder().
            setStart(ByteString.copyFrom(keyBegin)).
            setEnd(ByteString.copyFrom(keyEnd)).
            setWrite(true).
            build());
  }

  @Override
  public void set(byte[] key, byte[] value) {
    readOnlyTx.set(false);
    batcher.addSetValue(SetValueRequest.newBuilder().
        setKey(ByteString.copyFrom(key)).
        setValue(ByteString.copyFrom(value)).build());
  }

  @Override
  public void clear(byte[] key) {
    readOnlyTx.set(false);
    batcher.addClearKey(ClearKeyRequest.newBuilder().
        setKey(ByteString.copyFrom(key)).
        build());
  }

  @Override
  public void clear(byte[] beginKey, byte[] endKey) {
    readOnlyTx.set(false);
    batcher.addClearRange(ClearKeyRangeRequest.newBuilder().
        setStart(ByteString.copyFrom(beginKey)).
        setEnd(ByteString.copyFrom(endKey)).
        build());
  }

  @Override
  public void mutate(MutationType optype, byte[] key, byte[] param) {
    readOnlyTx.set(false);
    io.github.panghy.lionrock.proto.MutationType mutationType;
    switch (optype) {
      case AND:
      case OR:
      case XOR:
        throw new UnsupportedOperationException(optype + " is deprecated");
      case ADD:
        mutationType = io.github.panghy.lionrock.proto.MutationType.ADD;
        break;
      case BIT_AND:
        mutationType = io.github.panghy.lionrock.proto.MutationType.BIT_AND;
        break;
      case BIT_OR:
        mutationType = io.github.panghy.lionrock.proto.MutationType.BIT_OR;
        break;
      case BIT_XOR:
        mutationType = io.github.panghy.lionrock.proto.MutationType.BIT_XOR;
        break;
      case APPEND_IF_FITS:
        mutationType = io.github.panghy.lionrock.proto.MutationType.APPEND_IF_FITS;
        break;
      case MAX:
        mutationType = io.github.panghy.lionrock.proto.MutationType.MAX;
        break;
      case MIN:
        mutationType = io.github.panghy.lionrock.proto.MutationType.MIN;
        break;
      case SET_VERSIONSTAMPED_KEY:
        mutationType = io.github.panghy.lionrock.proto.MutationType.SET_VERSIONSTAMPED_KEY;
        break;
      case SET_VERSIONSTAMPED_VALUE:
        mutationType = io.github.panghy.lionrock.proto.MutationType.SET_VERSIONSTAMPED_VALUE;
        break;
      case BYTE_MIN:
        mutationType = io.github.panghy.lionrock.proto.MutationType.BYTE_MIN;
        break;
      case BYTE_MAX:
        mutationType = io.github.panghy.lionrock.proto.MutationType.BYTE_MAX;
        break;
      case COMPARE_AND_CLEAR:
        mutationType = io.github.panghy.lionrock.proto.MutationType.COMPARE_AND_CLEAR;
        break;
      default:
        throw new IllegalArgumentException("unsupported MutationType: " + optype);
    }
    batcher.addMutateValue(
        MutateValueRequest.newBuilder().
            setKey(ByteString.copyFrom(key)).
            setParam(ByteString.copyFrom(param)).
            setType(mutationType).
            build());
  }

  @Override
  public CompletableFuture<Void> commit() {
    if (commitStarted.getAndSet(true) || completed.get()) {
      return failedFuture(new FDBException("Operation issued while a commit was outstanding", 2017));
    } else if (cancelled.get()) {
      return failedFuture(new FDBException("Operation aborted because the transaction was cancelled", 1025));
    } else if (remoteError != null) {
      return failedFuture(new FDBException("remote server-side error encountered: " + remoteError.getMessage(), 4100));
    }
    if (readOnlyTx.get()) {
      versionStampFuture.completeExceptionally(NO_COMMIT_VERSION);
      close();
      commitFuture.complete(null);
    } else {
      conflictRangeBatcher.flush(true);
      batcher.flush(true);
      synchronized (requestSink) {
        requestSink.onNext(StreamingDatabaseRequest.newBuilder().setCommitTransaction(
                CommitTransactionRequest.newBuilder().build()).
            build());
      }
    }
    return commitFuture;
  }

  @Override
  public Long getCommittedVersion() {
    if (readOnlyTx.get() && commitStarted.get()) {
      return -1L;
    }
    if (commitResponse.get() == null) {
      throw new IllegalStateException("not yet committed");
    }
    return commitResponse.get().getCommittedVersion();
  }

  @Override
  public CompletableFuture<byte[]> getVersionstamp() {
    return versionStampFuture;
  }

  private <T> CompletableFuture<T> newCompletableFuture(String name) {
    if (shouldFail()) {
      return getFailedFuture();
    }
    NamedCompletableFuture<T> toReturn = new NamedCompletableFuture<>(name);
    synchronized (futures) {
      futures.add(toReturn);
    }
    return toReturn;
  }

  @Override
  public CompletableFuture<Long> getApproximateSize() {
    if (shouldFail()) {
      return getFailedFuture();
    }
    batcher.flush(true);
    conflictRangeBatcher.flush(true);
    CompletableFuture<Long> toReturn = newCompletableFuture("getApproximateSize");
    long curr = registerHandler(new StreamingDatabaseResponseVisitorStub() {

      @Override
      public void handleGetApproximateSize(GetApproximateSizeResponse resp) {
        toReturn.complete(resp.getSize());
      }
    }, toReturn);
    synchronized (requestSink) {
      requestSink.onNext(StreamingDatabaseRequest.newBuilder().
          setGetApproximateSize(
              GetApproximateSizeRequest.newBuilder().setSequenceId(curr).build()).
          build());
    }
    return toReturn;
  }

  @Override
  public CompletableFuture<Transaction> onError(Throwable e) {
    if ((e instanceof CompletionException || e instanceof ExecutionException) && e.getCause() != null) {
      e = e.getCause();
    }
    e = unwrapFdbException(e);
    if (!(e instanceof FDBException) || !FDBErrorCodes.isRetryable(((FDBException) e).getCode())) {
      return failedFuture(e);
    }
    // now we need to figure out the delay and such.
    // 2ms initial retry (5 * 2 ^ 1).
    long delayMs = (long) Math.min(remoteTransactionContext.getMaxRetryDelayMs(),
        1 * Math.pow(2.0, remoteTransactionContext.getAttempts()));
    // full-jitter.
    delayMs *= Math.random();
    // timeleft also needs to take into account the delay.
    long timeleftMs = remoteTransactionContext.getMillisecondsLeft();
    long timeLeftAfterDelayMs = timeleftMs - delayMs;
    if (remoteTransactionContext.getAttemptsLeft() <= 0 || timeLeftAfterDelayMs <= 0) {
      return failedFuture(
          new FDBException("Operation aborted because the transaction timed out", 1031));
    }
    // completeOnTimeout won't work since we only want to create the tranaction when we do time out.
    return this.<Transaction>newCompletableFuture("onError").
        orTimeout(delayMs, TimeUnit.MILLISECONDS).
        exceptionally(x ->
            new RemoteTransaction(remoteTransactionContext,
                timeleftMs == Long.MAX_VALUE ? -1 : timeLeftAfterDelayMs));
  }

  public static Throwable unwrapFdbException(Throwable e) {
    // ununwrap an fdb error within StatusRuntimeException if necessary.
    Throwable rootCause = Throwables.getRootCause(e);
    if (rootCause instanceof StatusRuntimeException) {
      Metadata trailers = ((StatusRuntimeException) rootCause).getTrailers();
      if (trailers != null) {
        if (trailers.containsKey(FDB_ERROR_CODE_KEY)) {
          String codeStr = trailers.get(FDB_ERROR_CODE_KEY);
          String message = rootCause.getMessage();
          if (trailers.containsKey(FDB_ERROR_MESSAGE_KEY)) {
            message = trailers.get(FDB_ERROR_MESSAGE_KEY);
          }
          int code = -1;
          try {
            code = Integer.parseInt(codeStr);
          } catch (NumberFormatException ignored) {
          }
          FDBException toReturn = new FDBException(message, code);
          toReturn.initCause(rootCause);
          return toReturn;
        }
      }
    }
    return e;
  }

  @Override
  public void cancel() {
    cancelled.set(true);
  }

  @Override
  public CompletableFuture<Void> watch(byte[] key) throws FDBException {
    if (shouldFail()) {
      return getFailedFuture();
    }
    batcher.flush(true);
    CompletableFuture<Void> toReturn = newCompletableFuture("watch");
    long curr = registerHandler(new StreamingDatabaseResponseVisitorStub() {

      @Override
      public void handleWatchKey(WatchKeyResponse resp) {
        toReturn.complete(null);
      }
    }, toReturn);
    synchronized (requestSink) {
      requestSink.onNext(StreamingDatabaseRequest.newBuilder().
          setWatchKey(WatchKeyRequest.newBuilder().
              setSequenceId(curr).
              setKey(ByteString.copyFrom(key)).build()).
          build());
    }
    return toReturn;
  }

  @Override
  public Database getDatabase() {
    return remoteTransactionContext.getDatabase();
  }

  @Override
  public void close() {
    if (!completed.getAndSet(true)) {
      // wait until all outstanding futures complete before closing the connection to the server.
      CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).
          whenComplete((unused, throwable) -> requestSink.onCompleted());
    }
  }

  @Override
  public boolean isSnapshot() {
    return false;
  }

  @Override
  public ReadTransaction snapshot() {
    return readTransaction;
  }

  @Override
  public CompletableFuture<Long> getReadVersion() {
    if (shouldFail()) {
      return getFailedFuture();
    }
    CompletableFuture<Long> toReturn = newCompletableFuture("getReadVersion");
    long curr = registerHandler(new StreamingDatabaseResponseVisitorStub() {
      @Override
      public void handleGetReadVersion(GetReadVersionResponse resp) {
        toReturn.complete(resp.getReadVersion());
      }
    }, toReturn);
    synchronized (requestSink) {
      requestSink.onNext(StreamingDatabaseRequest.newBuilder().
          setGetReadVersion(GetReadVersionRequest.newBuilder().
              setSequenceId(curr).
              build()).
          build());
    }
    return toReturn;
  }

  @Override
  public void setReadVersion(long version) {
    // this is done to simulate native client behavior, simply setting a read version will require us to commit since
    // setting to a past_version will throw on the commit.
    readOnlyTx.set(false);
    synchronized (requestSink) {
      requestSink.onNext(StreamingDatabaseRequest.newBuilder().setSetReadVersion(
              SetReadVersionRequest.newBuilder().setReadVersion(version).build()).
          build());
    }
  }

  @Override
  public CompletableFuture<byte[]> get(byte[] key) {
    return get(key, false);
  }

  private CompletableFuture<byte[]> get(byte[] key, boolean snapshot) {
    if (shouldFail()) {
      return getFailedFuture();
    }
    batcher.flush(true);
    CompletableFuture<byte[]> toReturn = newCompletableFuture("get: " + printable(key));
    long curr = registerHandler(new StreamingDatabaseResponseVisitorStub() {
      @Override
      public void handleGetValue(GetValueResponse resp) {
        toReturn.complete(resp.hasValue() ? resp.getValue().toByteArray() : null);
      }
    }, toReturn);
    synchronized (requestSink) {
      requestSink.onNext(StreamingDatabaseRequest.newBuilder().
          setGetValue(GetValueRequest.newBuilder().
              setKey(ByteString.copyFrom(key)).
              setSnapshot(snapshot).
              setSequenceId(curr).
              build()).
          build());
    }
    return toReturn;
  }

  @Override
  public CompletableFuture<byte[]> getKey(KeySelector selector) {
    return getKey(selector, false);
  }

  private CompletableFuture<byte[]> getKey(KeySelector selector, boolean snapshot) {
    if (shouldFail()) {
      return getFailedFuture();
    }
    batcher.flush(true);
    CompletableFuture<byte[]> toReturn = newCompletableFuture("getKey");
    long curr = registerHandler(new StreamingDatabaseResponseVisitorStub() {
      @Override
      public void handleGetKey(GetKeyResponse resp) {
        toReturn.complete(resp.hasKey() ? resp.getKey().toByteArray() : null);
      }
    }, toReturn);
    synchronized (requestSink) {
      requestSink.onNext(StreamingDatabaseRequest.newBuilder().
          setGetKey(GetKeyRequest.newBuilder().setKeySelector(keySelector(selector)).
              setSnapshot(snapshot).
              setSequenceId(curr).
              build()).
          build());
    }
    return toReturn;
  }

  @Override
  public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, int limit, boolean reverse,
                                          StreamingMode mode) {
    return getRange(begin, end, limit, reverse, mode, false);
  }

  private AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, int limit, boolean reverse,
                                           StreamingMode mode, boolean snapshot) {
    batcher.flush(true);
    io.github.panghy.lionrock.proto.StreamingMode streamingMode =
        io.github.panghy.lionrock.proto.StreamingMode.ITERATOR;
    switch (mode) {
      case WANT_ALL:
        streamingMode = io.github.panghy.lionrock.proto.StreamingMode.WANT_ALL;
        break;
      case ITERATOR:
        streamingMode = io.github.panghy.lionrock.proto.StreamingMode.ITERATOR;
        break;
      case EXACT:
        streamingMode = io.github.panghy.lionrock.proto.StreamingMode.EXACT;
        break;
    }
    io.github.panghy.lionrock.proto.StreamingMode finalStreamingMode = streamingMode;
    return new GrpcAsyncIterable<>(
        // key remover
        key -> RemoteTransaction.this.clear(key.getKey()),
        // resp to stream of KVs
        resp -> resp.getKeyValuesList().stream().
            map(x -> new KeyValue(x.getKey().toByteArray(), x.getValue().toByteArray())),
        // resp to isDone
        GetRangeResponse::getDone,
        this::newCompletableFuture,
        // fetch issuer
        (responseConsumer, failureConsumer) -> {
          long curr = demuxer.addHandler(new StreamingDatabaseResponseVisitorStub() {
            @Override
            public void handleGetRange(GetRangeResponse resp) {
              responseConsumer.accept(resp);
            }

            @Override
            public void handleOperationFailure(OperationFailureResponse resp) {
              failureConsumer.accept(resp);
            }
          });
          synchronized (requestSink) {
            requestSink.onNext(StreamingDatabaseRequest.newBuilder().
                setGetRange(GetRangeRequest.newBuilder().
                    setStartKeySelector(keySelector(begin)).
                    setEndKeySelector(keySelector(end)).
                    setLimit(limit).
                    setReverse(reverse).
                    setStreamingMode(finalStreamingMode).
                    setSnapshot(snapshot).
                    setSequenceId(curr).
                    build()).
                build());
          }
        },
        remoteTransactionContext.getExecutor());
  }

  @Override
  public CompletableFuture<Long> getEstimatedRangeSizeBytes(byte[] begin, byte[] end) {
    if (shouldFail()) {
      return getFailedFuture();
    }
    CompletableFuture<Long> toReturn = newCompletableFuture("getEstimatedRangeSizeBytes");
    long curr = registerHandler(new StreamingDatabaseResponseVisitorStub() {
      @Override
      public void handleGetEstimatedRangeSize(GetEstimatedRangeSizeResponse resp) {
        toReturn.complete(resp.getSize());
      }
    }, toReturn);
    synchronized (requestSink) {
      requestSink.onNext(StreamingDatabaseRequest.newBuilder().
          setGetEstimatedRangeSize(GetEstimatedRangeSizeRequest.newBuilder().
              setSequenceId(curr).
              setStart(ByteString.copyFrom(begin)).
              setEnd(ByteString.copyFrom(end)).
              build()).
          build());
    }
    return toReturn;
  }

  @Override
  public CompletableFuture<Long> getEstimatedRangeSizeBytes(Range range) {
    return getEstimatedRangeSizeBytes(range.begin, range.end);
  }

  @Override
  public TransactionOptions options() {
    return transactionOptions;
  }

  @Override
  public Executor getExecutor() {
    return remoteTransactionContext.getExecutor();
  }

  /**
   * Similar to {@code LocalityUtil.getBoundaryKeys(Transaction, byte[], byte[])}, yield an {@link AsyncIterable} for
   * boundary keys for a range.
   *
   * @param start Start of the range (inclusive).
   * @param end   End of the range (exclusive).
   * @return Iterable of boundary keys.
   */
  public AsyncIterable<byte[]> getBoundaryKeys(byte[] start, byte[] end) {
    return new GrpcAsyncIterable<>(
        // cannot clear keys.
        key -> {
          throw new UnsupportedOperationException();
        },
        // keys to byte[]
        resp -> resp.getKeysList().stream().map(ByteString::toByteArray),
        // isDone for resp
        GetBoundaryKeysResponse::getDone,
        this::newCompletableFuture,
        // fetch issuer.
        (responseConsumer, failureConsumer) -> {
          long curr = demuxer.addHandler(new StreamingDatabaseResponseVisitorStub() {
            @Override
            public void handleGetBoundaryKeys(GetBoundaryKeysResponse resp) {
              responseConsumer.accept(resp);
            }

            @Override
            public void handleOperationFailure(OperationFailureResponse resp) {
              failureConsumer.accept(resp);
            }
          });
          synchronized (requestSink) {
            requestSink.onNext(StreamingDatabaseRequest.newBuilder().
                setGetBoundaryKeys(GetBoundaryKeysRequest.newBuilder().
                    setStart(ByteString.copyFrom(start)).
                    setEnd(ByteString.copyFrom(end)).
                    setSequenceId(curr).
                    build()).
                build());
          }
        }, remoteTransactionContext.getExecutor());
  }

  /**
   * Similar to {@code LocalityUtil#getAddressesForKey(Transaction, byte[])}, return public server addresses for
   * storage servers that holds a particular key.
   *
   * @param key Key to fetch servers for.
   * @return Storage server addresses. Implementation specific.
   */
  public CompletableFuture<String[]> getAddressesForKey(byte[] key) {
    if (shouldFail()) {
      return getFailedFuture();
    }
    CompletableFuture<String[]> toReturn = newCompletableFuture("getAddressesForKey");
    long curr = registerHandler(new StreamingDatabaseResponseVisitorStub() {

      @Override
      public void handleGetAddressesForKey(GetAddressesForKeyResponse resp) {
        toReturn.complete(resp.getAddressesList().toArray(String[]::new));
      }
    }, toReturn);
    synchronized (requestSink) {
      requestSink.onNext(StreamingDatabaseRequest.newBuilder().
          setGetAddressesForKey(GetAddressesForKeyRequest.newBuilder().
              setKey(ByteString.copyFrom(key)).
              setSequenceId(curr).
              build()).
          build());
    }
    return toReturn;
  }

  private long registerHandler(StreamingDatabaseResponseVisitor visitor, CompletableFuture<?> future) {
    long curr = demuxer.addHandler(new StreamingDatabaseResponseVisitor() {

      @Override
      public void handleGetValue(GetValueResponse resp) {
        visitor.handleGetValue(resp);
      }

      @Override
      public void handleGetKey(GetKeyResponse resp) {
        visitor.handleGetKey(resp);
      }

      @Override
      public void handleGetApproximateSize(GetApproximateSizeResponse resp) {
        visitor.handleGetApproximateSize(resp);
      }

      @Override
      public void handleGetRange(GetRangeResponse resp) {
        visitor.handleGetRange(resp);
      }

      @Override
      public void handleGetReadVersion(GetReadVersionResponse resp) {
        visitor.handleGetReadVersion(resp);
      }

      @Override
      public void handleWatchKey(WatchKeyResponse resp) {
        visitor.handleWatchKey(resp);
      }

      @Override
      public void handleGetEstimatedRangeSize(GetEstimatedRangeSizeResponse resp) {
        visitor.handleGetEstimatedRangeSize(resp);
      }

      @Override
      public void handleGetBoundaryKeys(GetBoundaryKeysResponse resp) {
        visitor.handleGetBoundaryKeys(resp);
      }

      @Override
      public void handleGetAddressesForKey(GetAddressesForKeyResponse resp) {
        visitor.handleGetAddressesForKey(resp);
      }

      @Override
      public void handleOperationFailure(OperationFailureResponse resp) {
        future.completeExceptionally(new FDBException(resp.getMessage(), (int) resp.getCode()));
      }
    });
    if (future instanceof NamedCompletableFuture) {
      ((NamedCompletableFuture<?>) future).addBaggage("seq_id", String.valueOf(curr));
    }
    return curr;
  }

  @Override
  protected void finalize() throws Throwable {
    try {
      close();
    } finally {
      super.finalize();
    }
  }

  /**
   * @return whether {@link #getFailedFuture()} would produce a failed future.
   */
  private boolean shouldFail() {
    return commitStarted.get() || completed.get() || cancelled.get() || remoteError != null;
  }

  /**
   * Assert that the user can interact with this {@link RemoteTransaction}.
   */
  private <T> CompletableFuture<T> getFailedFuture() {
    if (commitStarted.get() || completed.get()) {
      return failedFuture(new FDBException("Operation issued while a commit was outstanding", 2017));
    } else if (cancelled.get()) {
      return failedFuture(new FDBException("Operation aborted because the transaction was cancelled", 1025));
    } else if (remoteError != null) {
      return failedFuture(new FDBException("remote server-side error encountered: " + remoteError.getMessage(), 4100));
    }
    throw new IllegalStateException("transaction is still active");
  }

  /**
   * Takes an apple foundationdb {@link KeySelector} and transforms it to a lionrock
   * {@link io.github.panghy.lionrock.proto.KeySelector}. It is ugly since orEqual is not visible.
   *
   * @param keySelector FoundationDB {@link KeySelector}.
   * @return Lionrock {@link io.github.panghy.lionrock.proto.KeySelector}.
   */
  private io.github.panghy.lionrock.proto.KeySelector keySelector(KeySelector keySelector) {
    // keySelector orEquals() is package private. while that gets fixed, we'll have to do something ugly.
    String kSStr = keySelector.toString();
    byte[] key = keySelector.getKey();
    String printableKey = printable(key);
    String orEqualsTrue = String.format("(%s, %s, %d)", printableKey, true, keySelector.getOffset());
    String orEqualsFalse = String.format("(%s, %s, %d)", printableKey, false, keySelector.getOffset());
    if (kSStr.equals(orEqualsTrue)) {
      return io.github.panghy.lionrock.proto.KeySelector.newBuilder().
          setKey(ByteString.copyFrom(key)).
          setOffset(keySelector.getOffset()).
          setOrEqual(true).build();
    } else if (kSStr.equals(orEqualsFalse)) {
      return io.github.panghy.lionrock.proto.KeySelector.newBuilder().
          setKey(ByteString.copyFrom(key)).
          setOffset(keySelector.getOffset()).
          setOrEqual(false).build();
    } else {
      throw new IllegalArgumentException("KeySelector cannot be probed for orEquals value: " + kSStr);
    }
  }

  /**
   * Passed from {@link RemoteDatabase} to {@link RemoteTransaction} for context related to the database that we are
   * interacting with.
   */
  public interface RemoteTransactionContext {
    /**
     * @return Name of the transaction. Allows for better debugging and traceability.
     */
    String getName();

    /**
     * @return {@link Executor} related to how async callbacks might be handled.
     */
    Executor getExecutor();

    /**
     * @return The gRPC stub that allows us to issue new transactions (see {@link #onError(Throwable)}.
     */
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub getStub();

    /**
     * @return The {@link RemoteDatabase} that this {@link RemoteTransaction} is associated with.
     */
    RemoteDatabase getDatabase();

    /**
     * @return The number of attempts we have tried this transaction.
     */
    long getAttempts();

    /**
     * Increment the attempts we have tried.
     */
    void incrementAttempts();

    /**
     * @return Time left for the transaction before the deadline.
     */
    long getMillisecondsLeft();

    /**
     * @return Attempts left for this transaction.
     */
    long getAttemptsLeft();

    /**
     * @return Max delay between retries.
     */
    long getMaxRetryDelayMs();

    /**
     * Set the timeout of the transaction.
     */
    void setTimeout(long timeoutMs);

    /**
     * Set the max retry delay of the transaction.
     */
    void setMaxRetryDelay(long delayMs);

    /**
     * Set the retry attempts of the transaction.
     */
    void setRetryLimit(long limit);
  }
}
