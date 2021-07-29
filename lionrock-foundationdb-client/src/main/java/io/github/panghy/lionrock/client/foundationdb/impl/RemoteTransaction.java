package io.github.panghy.lionrock.client.foundationdb.impl;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.google.protobuf.ByteString;
import io.github.panghy.lionrock.client.foundationdb.impl.collections.GrpcAsyncIterable;
import io.github.panghy.lionrock.client.foundationdb.mixins.ReadTransactionMixin;
import io.github.panghy.lionrock.client.foundationdb.mixins.TransactionMixin;
import io.github.panghy.lionrock.proto.*;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link Transaction} that's backed by a gRPC streaming call.
 *
 * @author Clement Pang
 */
public class RemoteTransaction implements TransactionMixin {

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
  private final AtomicLong sequenceId = new AtomicLong();
  private final RemoteTransactionContext remoteTransactionContext;
  /**
   * Instance of {@link TransactionOptions} for this transaction context.
   */
  private final TransactionOptions transactionOptions = new TransactionOptions(new OptionConsumer() {
    @Override
    public void setOption(int code, byte[] parameter) {
      assertTransactionState();
      if (code == 500) {
        // timeout.
        long timeoutMs = ByteBuffer.wrap(parameter).order(ByteOrder.LITTLE_ENDIAN).getLong();
        remoteTransactionContext.setTimeout(timeoutMs);
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
        requestSink.onNext(StreamingDatabaseRequest.newBuilder().
            setSetTransactionOption(builder.build()).build());
      }
    }
  });
  /**
   * Instance of {@link ReadTransaction} that allows for snapshot reads.
   */
  private final RemoteReadTransaction readTransaction = new RemoteReadTransaction();
  private final List<CompletableFuture<?>> futures = new ArrayList<>();
  private final CompletableFuture<Void> commitFuture = newCompletableFuture();

  private Throwable remoteError;

  public RemoteTransaction(RemoteTransactionContext remoteTransactionContext, long timeoutMs) {
    TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub = remoteTransactionContext.getStub();
    if (timeoutMs > 0) {
      stub = stub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS);
    }
    remoteTransactionContext.incrementAttempts();
    this.requestSink = stub.executeTransaction(new StreamObserver<>() {
      @Override
      public void onNext(StreamingDatabaseResponse value) {
        //noinspection StatementWithEmptyBody
        if (cancelled.get() || remoteError != null) {
          // ignore if client-side cancelled or server-side errored out.
        } else if (value.hasCommitTransaction()) {
          commitResponse.set(value.getCommitTransaction());
          completed.set(true);
          commitFuture.completeAsync(() -> null, getExecutor());
        } else {
          demuxer.accept(value);
        }
      }

      @Override
      public void onError(Throwable t) {
        futures.forEach(x -> x.completeExceptionally(t));
        remoteError = t;
      }

      @Override
      public void onCompleted() {
        requestSink.onCompleted();
        completed.set(true);
      }
    });
    // start the transaction with the server.
    this.requestSink.onNext(StreamingDatabaseRequest.newBuilder().
        setStartTransaction(StartTransactionRequest.newBuilder().
            setClientIdentifier(remoteTransactionContext.getDatabase().getClientIdentifier()).
            setDatabaseName(remoteTransactionContext.getDatabase().getDatabaseName()).
            setName(remoteTransactionContext.getName()).
            build()).build());
    this.remoteTransactionContext = remoteTransactionContext;
    this.demuxer = new SequenceResponseDemuxer(remoteTransactionContext.getExecutor());
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
      return RemoteTransaction.this.get(key);
    }

    @Override
    public CompletableFuture<byte[]> getKey(KeySelector selector) {
      return RemoteTransaction.this.getKey(selector);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, int limit, boolean reverse,
                                            StreamingMode mode) {
      return RemoteTransaction.this.getRange(begin, end, limit, reverse, mode);
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
  public void addReadConflictRange(byte[] keyBegin, byte[] keyEnd) {
    assertTransactionState();
    requestSink.onNext(StreamingDatabaseRequest.newBuilder().setAddConflictRange(
        AddConflictRangeRequest.newBuilder().
            setStart(ByteString.copyFrom(keyBegin)).
            setEnd(ByteString.copyFrom(keyEnd)).
            setWrite(false).
            build()).build());
  }

  @Override
  public void addWriteConflictRange(byte[] keyBegin, byte[] keyEnd) {
    assertTransactionState();
    requestSink.onNext(StreamingDatabaseRequest.newBuilder().setAddConflictRange(
        AddConflictRangeRequest.newBuilder().
            setStart(ByteString.copyFrom(keyBegin)).
            setEnd(ByteString.copyFrom(keyEnd)).
            setWrite(true).
            build()).build());
  }

  @Override
  public void set(byte[] key, byte[] value) {
    assertTransactionState();
    requestSink.onNext(StreamingDatabaseRequest.newBuilder().setSetValue(
            SetValueRequest.newBuilder().
                setKey(ByteString.copyFrom(key)).
                setValue(ByteString.copyFrom(value)).build()).
        build());
  }

  @Override
  public void clear(byte[] key) {
    assertTransactionState();
    requestSink.onNext(StreamingDatabaseRequest.newBuilder().setClearKey(
            ClearKeyRequest.newBuilder().
                setKey(ByteString.copyFrom(key)).
                build()).
        build());
  }

  @Override
  public void clear(byte[] beginKey, byte[] endKey) {
    assertTransactionState();
    requestSink.onNext(StreamingDatabaseRequest.newBuilder().setClearRange(
            ClearKeyRangeRequest.newBuilder().
                setStart(ByteString.copyFrom(beginKey)).
                setEnd(ByteString.copyFrom(endKey)).
                build()).
        build());
  }

  @Override
  public void mutate(MutationType optype, byte[] key, byte[] param) {
    assertTransactionState();
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
    requestSink.onNext(StreamingDatabaseRequest.newBuilder().setMutateValue(
            MutateValueRequest.newBuilder().
                setKey(ByteString.copyFrom(key)).
                setParam(ByteString.copyFrom(param)).
                setType(mutationType).
                build()).
        build());
  }

  @Override
  public CompletableFuture<Void> commit() {
    assertTransactionState();
    if (commitStarted.getAndSet(true)) {
      throw new IllegalStateException("commit already started");
    }
    requestSink.onNext(StreamingDatabaseRequest.newBuilder().setCommitTransaction(
            CommitTransactionRequest.newBuilder().build()).
        build());
    return commitFuture;
  }

  @Override
  public Long getCommittedVersion() {
    if (commitResponse.get() == null) {
      throw new IllegalStateException("not yet committed");
    }
    return commitResponse.get().getCommittedVersion();
  }

  @Override
  public CompletableFuture<byte[]> getVersionstamp() {
    assertTransactionState();
    CompletableFuture<byte[]> toReturn = newCompletableFuture();
    long curr = registerHandler(new StreamingDatabaseResponseVisitorStub() {
      @Override
      public void handleGetVersionstamp(GetVersionstampResponse resp) {
        toReturn.completeAsync(() -> resp.getVersionstamp().toByteArray(), getExecutor());
      }
    }, toReturn);
    requestSink.onNext(StreamingDatabaseRequest.newBuilder().
        setGetVersionstamp(
            GetVersionstampRequest.newBuilder().setSequenceId(curr).build()).
        build());
    return toReturn;
  }

  private <T> CompletableFuture<T> newCompletableFuture() {
    CompletableFuture<T> toReturn = new CompletableFuture<>();
    synchronized (futures) {
      futures.add(toReturn);
    }
    return toReturn;
  }

  @Override
  public CompletableFuture<Long> getApproximateSize() {
    assertTransactionState();
    CompletableFuture<Long> toReturn = newCompletableFuture();
    long curr = registerHandler(new StreamingDatabaseResponseVisitorStub() {

      @Override
      public void handleGetApproximateSize(GetApproximateSizeResponse resp) {
        toReturn.completeAsync(resp::getSize, getExecutor());
      }
    }, toReturn);
    requestSink.onNext(StreamingDatabaseRequest.newBuilder().
        setGetApproximateSize(
            GetApproximateSizeRequest.newBuilder().setSequenceId(curr).build()).
        build());
    return toReturn;
  }

  @Override
  public CompletableFuture<Transaction> onError(Throwable e) {
    if ((e instanceof CompletionException || e instanceof ExecutionException) && e.getCause() != null) {
      e = e.getCause();
    }
    // ununwrap an fdb error within StatusRuntimeException if necessary.
    if (e instanceof StatusRuntimeException) {
      Metadata trailers = ((StatusRuntimeException) e).getTrailers();
      if (trailers != null) {
        if (trailers.containsKey(FDB_ERROR_CODE_KEY)) {
          String codeStr = trailers.get(FDB_ERROR_CODE_KEY);
          String message = e.getMessage();
          if (trailers.containsKey(FDB_ERROR_MESSAGE_KEY)) {
            message = trailers.get(FDB_ERROR_MESSAGE_KEY);
          }
          int code = -1;
          try {
            code = Integer.parseInt(codeStr);
          } catch (NumberFormatException ignored) {
          }
          e = new FDBException(message, code);
        }
      }
    }
    if (!(e instanceof FDBException) || !FDBErrorCodes.isRetryable(((FDBException) e).getCode())) {
      return CompletableFuture.failedFuture(e);
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
      return CompletableFuture.failedFuture(
          new FDBException("Operation aborted because the transaction timed out", 1031));
    }
    // completeOnTimeout won't work since we only want to create the tranaction when we do time out.
    return this.<Transaction>newCompletableFuture().
        orTimeout(delayMs, TimeUnit.MILLISECONDS).
        exceptionally(x ->
            new RemoteTransaction(remoteTransactionContext,
                timeleftMs == Long.MAX_VALUE ? -1 : timeLeftAfterDelayMs));
  }

  @Override
  public void cancel() {
    assertTransactionState();
    cancelled.set(true);
  }

  @Override
  public CompletableFuture<Void> watch(byte[] key) throws FDBException {
    assertTransactionState();
    CompletableFuture<Void> toReturn = newCompletableFuture();
    long curr = registerHandler(new StreamingDatabaseResponseVisitorStub() {

      @Override
      public void handleGetWatchKey(WatchKeyResponse resp) {
        toReturn.completeAsync(() -> null, getExecutor());
      }
    }, toReturn);
    requestSink.onNext(StreamingDatabaseRequest.newBuilder().
        setWatchKey(WatchKeyRequest.newBuilder().
            setSequenceId(curr).
            setKey(ByteString.copyFrom(key)).build()).
        build());
    return toReturn;
  }

  @Override
  public Database getDatabase() {
    return remoteTransactionContext.getDatabase();
  }

  @Override
  public void close() {
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
    assertTransactionState();
    CompletableFuture<Long> toReturn = newCompletableFuture();
    long curr = registerHandler(new StreamingDatabaseResponseVisitorStub() {
      @Override
      public void handleGetReadVersion(GetReadVersionResponse resp) {
        toReturn.completeAsync(resp::getReadVersion, getExecutor());
      }
    }, toReturn);
    requestSink.onNext(StreamingDatabaseRequest.newBuilder().
        setGetReadVersion(GetReadVersionRequest.newBuilder().setSequenceId(curr).
            build()).
        build());
    return toReturn;
  }

  @Override
  public void setReadVersion(long version) {
    assertTransactionState();
    requestSink.onNext(StreamingDatabaseRequest.newBuilder().setSetReadVersion(
            SetReadVersionRequest.newBuilder().setReadVersion(version).build()).
        build());
  }

  @Override
  public CompletableFuture<byte[]> get(byte[] key) {
    assertTransactionState();
    CompletableFuture<byte[]> toReturn = newCompletableFuture();
    long curr = registerHandler(new StreamingDatabaseResponseVisitorStub() {
      @Override
      public void handleGetValue(GetValueResponse resp) {
        toReturn.completeAsync(() -> resp.hasValue() ? resp.getValue().toByteArray() : null, getExecutor());
      }
    }, toReturn);
    requestSink.onNext(StreamingDatabaseRequest.newBuilder().
        setGetValue(GetValueRequest.newBuilder().setKey(ByteString.copyFrom(key)).
            setSequenceId(curr).
            build()).
        build());
    return toReturn;
  }

  @Override
  public CompletableFuture<byte[]> getKey(KeySelector selector) {
    assertTransactionState();
    CompletableFuture<byte[]> toReturn = newCompletableFuture();
    long curr = registerHandler(new StreamingDatabaseResponseVisitorStub() {
      @Override
      public void handleGetKey(GetKeyResponse resp) {
        toReturn.completeAsync(() -> resp.hasKey() ? resp.getKey().toByteArray() : null, getExecutor());
      }
    }, toReturn);
    requestSink.onNext(StreamingDatabaseRequest.newBuilder().
        setGetKey(GetKeyRequest.newBuilder().setKeySelector(keySelector(selector)).
            setSequenceId(curr).
            build()).
        build());
    return toReturn;
  }

  @Override
  public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, int limit, boolean reverse,
                                          StreamingMode mode) {
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
        // fetch issuer
        (responseConsumer, failureConsumer) -> {
          assertTransactionState();
          long curr = sequenceId.getAndIncrement();
          demuxer.addHandler(curr, new StreamingDatabaseResponseVisitorStub() {
            @Override
            public void handleGetRange(GetRangeResponse resp) {
              responseConsumer.accept(resp);
            }

            @Override
            public void handleOperationFailure(OperationFailureResponse resp) {
              failureConsumer.accept(resp);
            }
          });
          requestSink.onNext(StreamingDatabaseRequest.newBuilder().
              setGetRange(GetRangeRequest.newBuilder().
                  setStartKeySelector(keySelector(begin)).
                  setEndKeySelector(keySelector(end)).
                  setLimit(limit).
                  setReverse(reverse).
                  setStreamingMode(finalStreamingMode).
                  setSequenceId(curr).
                  build()).
              build());
        },
        remoteTransactionContext.getExecutor());
  }

  @Override
  public CompletableFuture<Long> getEstimatedRangeSizeBytes(byte[] begin, byte[] end) {
    assertTransactionState();
    CompletableFuture<Long> toReturn = newCompletableFuture();
    long curr = registerHandler(new StreamingDatabaseResponseVisitorStub() {
      @Override
      public void handleGetEstimatedRangeSize(GetEstimatedRangeSizeResponse resp) {
        toReturn.completeAsync(resp::getSize, getExecutor());
      }
    }, toReturn);
    requestSink.onNext(StreamingDatabaseRequest.newBuilder().
        setGetEstimatedRangeSize(GetEstimatedRangeSizeRequest.newBuilder().
            setSequenceId(curr).
            setStart(ByteString.copyFrom(begin)).
            setEnd(ByteString.copyFrom(end)).
            build()).
        build());
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
        // fetch issuer.
        (responseConsumer, failureConsumer) -> {
          assertTransactionState();
          long curr = sequenceId.getAndIncrement();
          demuxer.addHandler(curr, new StreamingDatabaseResponseVisitorStub() {
            @Override
            public void handleGetBoundaryKeys(GetBoundaryKeysResponse resp) {
              responseConsumer.accept(resp);
            }

            @Override
            public void handleOperationFailure(OperationFailureResponse resp) {
              failureConsumer.accept(resp);
            }
          });
          requestSink.onNext(StreamingDatabaseRequest.newBuilder().
              setGetBoundaryKeys(GetBoundaryKeysRequest.newBuilder().
                  setStart(ByteString.copyFrom(start)).
                  setEnd(ByteString.copyFrom(end)).
                  setSequenceId(curr).
                  build()).
              build());
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
    assertTransactionState();
    CompletableFuture<String[]> toReturn = newCompletableFuture();
    long curr = registerHandler(new StreamingDatabaseResponseVisitorStub() {

      @Override
      public void handleGetAddressesForKey(GetAddressesForKeyResponse resp) {
        toReturn.completeAsync(() -> resp.getAddressesList().toArray(String[]::new), getExecutor());
      }
    }, toReturn);
    requestSink.onNext(StreamingDatabaseRequest.newBuilder().
        setGetAddressesForKey(GetAddressesForKeyRequest.newBuilder().
            setKey(ByteString.copyFrom(key)).
            setSequenceId(curr).
            build()).
        build());
    return toReturn;
  }

  private long registerHandler(StreamingDatabaseResponseVisitor visitor, CompletableFuture<?> future) {
    long curr = this.sequenceId.getAndIncrement();
    demuxer.addHandler(curr, new StreamingDatabaseResponseVisitor() {

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
      public void handleGetVersionstamp(GetVersionstampResponse resp) {
        visitor.handleGetVersionstamp(resp);
      }

      @Override
      public void handleGetReadVersion(GetReadVersionResponse resp) {
        visitor.handleGetReadVersion(resp);
      }

      @Override
      public void handleGetWatchKey(WatchKeyResponse resp) {
        visitor.handleGetWatchKey(resp);
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
    return curr;
  }

  /**
   * Assert that the user can interact with this {@link RemoteTransaction}.
   */
  private void assertTransactionState() {
    if (commitStarted.get()) {
      throw new IllegalStateException("commit already started");
    }
    if (cancelled.get()) {
      throw new IllegalStateException("transaction already cancelled");
    }
    if (completed.get()) {
      throw new IllegalStateException("transaction already completed");
    }
    if (remoteError != null) {
      if (remoteError instanceof RuntimeException) {
        throw (RuntimeException) remoteError;
      }
      throw new RuntimeException("server-side error encountered", remoteError);
    }
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
    String printableKey = ByteArrayUtil.printable(key);
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
