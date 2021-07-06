package io.github.panghy.lionrock.foundationdb;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.google.protobuf.ByteString;
import io.github.panghy.lionrock.proto.MutationType;
import io.github.panghy.lionrock.proto.*;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.lognet.springboot.grpc.GRpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static com.apple.foundationdb.tuple.ByteArrayUtil.printable;
import static java.util.concurrent.CompletableFuture.completedFuture;

@SpringBootApplication
@GRpcService
@Component
public class FoundationDbGrpcFacade extends TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreImplBase {

  Logger logger = LoggerFactory.getLogger(FoundationDbGrpcFacade.class);

  @Autowired
  Configuration config;
  @Autowired
  Tracer tracer;

  Map<String, Configuration.Cluster> clusterMap = new HashMap<>();
  Map<String, Database> databaseMap = new HashMap<>();

  @PostConstruct
  public void init() {
    logger.info("Configuring: " + config.getClusters().size() + " clusters for gRPC access " +
        "(FDB API Version: " + config.getFdbVersion() + ")");
    FDB.selectAPIVersion(config.getFdbVersion());
    for (Configuration.Cluster cluster : config.getClusters()) {
      clusterMap.put(cluster.getName(), cluster);
      Database fdb = FDB.instance().open(cluster.getClusterFile());
      databaseMap.put(cluster.getName(), fdb);
    }
  }

  @Override
  public void execute(DatabaseRequest request, StreamObserver<DatabaseResponse> responseObserver) {
    // fill-in additional details to the overall span
    Span overallSpan = tracer.currentSpan();
    if (overallSpan != null) {
      overallSpan.tag("client", request.getClientIdentifier()).
          tag("database_name", request.getDatabaseName()).
          tag("name", request.getName());
    }
    if (logger.isDebugEnabled()) {
      String msg = "Executing DatabaseRequest from: " + request.getClientIdentifier() + " on database: " +
          request.getDatabaseName() + " named: " + request.getName();
      logger.debug(msg);
      if (overallSpan != null) {
        overallSpan.event(msg);
      }
    }
    Database db = databaseMap.get(request.getDatabaseName());
    if (db == null) {
      StatusRuntimeException toThrow = Status.INVALID_ARGUMENT.
          withDescription("cannot find database named: " + request.getDatabaseName()).
          asRuntimeException();
      responseObserver.onError(toThrow);
      throw toThrow;
    }
    Context rpcContext = Context.current();
    if (request.hasGetValue()) {
      if (overallSpan != null) {
        overallSpan.tag("request", "get_value");
      }
      if (logger.isDebugEnabled()) {
        String msg = "GetValueRequest on: " + printable(request.getGetValue().getKey().toByteArray());
        logger.debug(msg);
        if (overallSpan != null) {
          overallSpan.event(msg);
        }
      }
      handleException(db.runAsync(tx -> {
        prepareTx(request, rpcContext, tx);
        return tx.get(request.getGetValue().getKey().toByteArray());
      }), overallSpan, responseObserver, "failed to get key").thenAccept(val -> {
        try (Tracer.SpanInScope ignored = tracer.withSpan(overallSpan)) {
          GetValueResponse.Builder build = GetValueResponse.newBuilder();
          if (logger.isDebugEnabled()) {
            String msg = "GetValueRequest on: " +
                printable(request.getGetValue().getKey().toByteArray()) + " is: " +
                printable(val);
            logger.debug(msg);
            if (overallSpan != null) {
              overallSpan.event(msg);
            }
          }
          if (val != null) {
            build.setValue(ByteString.copyFrom(val));
          }
          responseObserver.onNext(DatabaseResponse.newBuilder().
              setGetValue(build.build()).
              build());
          responseObserver.onCompleted();
        }
      });
    } else if (request.hasSetValue()) {
      if (overallSpan != null) {
        overallSpan.tag("request", "set_value");
      }
      if (logger.isDebugEnabled()) {
        String msg = "SetValueRequest on: " + printable(request.getSetValue().getKey().toByteArray()) + " => " +
            printable(request.getSetValue().getValue().toByteArray());
        logger.debug(msg);
        if (overallSpan != null) {
          overallSpan.event(msg);
        }
      }
      handleCommittedTransaction(
          handleException(
              db.runAsync(tx -> {
                prepareTx(request, rpcContext, tx);
                tx.set(request.getSetValue().getKey().toByteArray(), request.getSetValue().getValue().toByteArray());
                return completedFuture(tx);
              }), overallSpan, responseObserver, "failed to set key"),
          responseObserver);
    } else if (request.hasClearKey()) {
      if (overallSpan != null) {
        overallSpan.tag("request", "clear_key");
      }
      if (logger.isDebugEnabled()) {
        String msg = "ClearKeyRequest on: " + printable(request.getClearKey().getKey().toByteArray());
        overallSpan.event(msg);
        logger.debug(msg);
      }
      handleCommittedTransaction(
          handleException(
              db.runAsync(tx -> {
                prepareTx(request, rpcContext, tx);
                tx.clear(request.getClearKey().getKey().toByteArray());
                return completedFuture(tx);
              }), overallSpan, responseObserver, "failed to set key"),
          responseObserver);
    } else if (request.hasClearRange()) {
      if (overallSpan != null) {
        overallSpan.tag("request", "clear_range");
      }
      if (logger.isDebugEnabled()) {
        String msg = "ClearRangeRequest on: " + printable(request.getClearRange().getStart().toByteArray()) + " => " +
            printable(request.getClearRange().getEnd().toByteArray());
        logger.debug(msg);
        if (overallSpan != null) {
          overallSpan.event(msg);
        }
      }
      handleCommittedTransaction(
          handleException(
              db.runAsync(tx -> {
                prepareTx(request, rpcContext, tx);
                tx.clear(request.getClearRange().getStart().toByteArray(),
                    request.getClearRange().getEnd().toByteArray());
                return completedFuture(tx);
              }), overallSpan, responseObserver, "failed to set key"),
          responseObserver);
    } else if (request.hasGetRange()) {
      if (overallSpan != null) {
        overallSpan.tag("request", "get_range");
      }
      GetRangeRequest req = request.getGetRange();
      KeySelector start;
      if (req.hasStartBytes()) {
        start = new KeySelector(req.getStartBytes().toByteArray(), true, 0);
      } else if (req.hasStartKeySelector()) {
        start = new KeySelector(req.getStartKeySelector().getKey().toByteArray(),
            req.getStartKeySelector().getOrEqual(), req.getStartKeySelector().getOffset());
      } else {
        throw Status.INVALID_ARGUMENT.withDescription("must have start").asRuntimeException();
      }
      KeySelector end;
      if (req.hasEndBytes()) {
        end = new KeySelector(req.getEndBytes().toByteArray(), true, 0);
      } else if (req.hasEndKeySelector()) {
        end = new KeySelector(req.getEndKeySelector().getKey().toByteArray(),
            req.getEndKeySelector().getOrEqual(), req.getEndKeySelector().getOffset());
      } else {
        throw Status.INVALID_ARGUMENT.withDescription("must have end").asRuntimeException();
      }
      if (logger.isDebugEnabled()) {
        String msg = "GetRangeRequest from: " + start + " to: " + end + " reverse: " + req.getReverse() +
            " limit: " + req.getLimit() + " mode: " + req.getStreamingMode();
        logger.debug(msg);
        if (overallSpan != null) {
          overallSpan.event(msg);
        }
      }
      StreamingMode mode = StreamingMode.ITERATOR;
      switch (req.getStreamingMode()) {
        case ITERATOR:
          mode = StreamingMode.ITERATOR;
          break;
        case WANT_ALL:
          mode = StreamingMode.WANT_ALL;
          break;
        case EXACT:
          mode = StreamingMode.EXACT;
          break;
        case UNRECOGNIZED:
          mode = StreamingMode.ITERATOR;
          break;
      }
      StreamingMode finalMode = mode;
      handleException(db.runAsync(tx -> {
            prepareTx(request, rpcContext, tx);
            if (request.getReadVersion() > 0) {
              tx.setReadVersion(request.getReadVersion());
            }
            return tx.getRange(start, end, req.getLimit(), req.getReverse(), finalMode).asList();
          }),
          overallSpan, responseObserver, "failed to get range").thenAccept(results -> {
        try (Tracer.SpanInScope ignored = tracer.withSpan(overallSpan)) {
          GetRangeResponse.Builder build = GetRangeResponse.newBuilder();
          if (logger.isDebugEnabled()) {
            String msg = "GetRangeRequest from: " + start + " to: " + end + " reverse: " + req.getReverse() +
                " limit: " + req.getLimit() + " mode: " + req.getStreamingMode() + " got: " + results.size() + " rows";
            logger.debug(msg);
            if (overallSpan != null) {
              overallSpan.event(msg);
            }
          }
          if (overallSpan != null) {
            overallSpan.tag("rows", String.valueOf(results.size()));
          }
          List<io.github.panghy.lionrock.proto.KeyValue> keyValues = new ArrayList<>(results.size());
          for (KeyValue result : results) {
            keyValues.add(io.github.panghy.lionrock.proto.KeyValue.newBuilder().
                setKey(ByteString.copyFrom(result.getKey())).
                setValue(ByteString.copyFrom(result.getValue())).build());
          }
          build.addAllKeyValues(keyValues);
          build.setDone(true);
          responseObserver.onNext(DatabaseResponse.newBuilder().
              setGetRange(build.build()).
              build());
          responseObserver.onCompleted();
        }
      });
    } else if (request.hasMutateValue()) {
      if (overallSpan != null) {
        overallSpan.tag("request", "mutate_value");
      }
      if (logger.isDebugEnabled()) {
        String msg = "MutateValueRequest on: " +
            printable(request.getMutateValue().getKey().toByteArray()) + " => " +
            printable(request.getMutateValue().getParam().toByteArray()) + " with: " +
            request.getMutateValue().getType();
        logger.debug(msg);
        if (overallSpan != null) {
          overallSpan.event(msg);
        }
      }
      handleCommittedTransaction(
          handleException(
              db.runAsync(tx -> {
                prepareTx(request, rpcContext, tx);
                tx.mutate(getMutationType(request.getMutateValue().getType()),
                    request.getMutateValue().getKey().toByteArray(),
                    request.getMutateValue().getParam().toByteArray());
                return completedFuture(tx);
              }), overallSpan, responseObserver, "failed to mutate key"),
          responseObserver);
    }
  }

  @Override
  public StreamObserver<StreamingDatabaseRequest> executeTransaction(
      StreamObserver<StreamingDatabaseResponse> responseObserver) {
    Context rpcContext = Context.current();
    Span overallSpan = this.tracer.currentSpan();
    return new StreamObserver<>() {

      private final AtomicReference<StartTransactionRequest> startRequest = new AtomicReference<>();
      private final AtomicBoolean commitStarted = new AtomicBoolean();
      private final AtomicLong rowsWritten = new AtomicLong();
      private final AtomicLong rowsMutated = new AtomicLong();
      private final AtomicLong rowsRead = new AtomicLong();
      private final AtomicLong getReadVersion = new AtomicLong();
      private final AtomicLong rangeGets = new AtomicLong();
      private final AtomicLong rangeGetBatches = new AtomicLong();
      private final AtomicLong clears = new AtomicLong();
      private final AtomicLong readConflictAdds = new AtomicLong();
      private final AtomicLong writeConflictAdds = new AtomicLong();
      private final AtomicLong getVersionstamp = new AtomicLong();
      private volatile Transaction tx;
      /**
       * {@link CompletableFuture}s chain that is created during the livetime of the transaction but may not resolve
       * until <i>some</i> time later after the transaction is closed and destroyed. Examples include watches and the
       * retrieval of the versionstamp after a commit.
       */
      private volatile CompletableFuture<?> postCommitFutures = completedFuture(null);

      @Override
      public void onNext(StreamingDatabaseRequest value) {
        if (value.hasStartTransaction()) {
          StartTransactionRequest startRequest = this.startRequest.updateAndGet(startTransactionRequest -> {
            if (startTransactionRequest != null) {
              StatusRuntimeException toThrow = Status.INVALID_ARGUMENT.
                  withDescription("cannot send StartTransactionRequest twice").
                  asRuntimeException();
              synchronized (responseObserver) {
                responseObserver.onError(toThrow);
              }
              throw toThrow;
            }
            return value.getStartTransaction();
          });
          if (logger.isDebugEnabled()) {
            String msg = "Starting transaction " + startRequest.getName() + " against db: " +
                startRequest.getDatabaseName();
            logger.debug(msg);
            overallSpan.event(msg);
          }
          Database db = databaseMap.get(startRequest.getDatabaseName());
          if (db == null) {
            StatusRuntimeException toThrow = Status.INVALID_ARGUMENT.
                withDescription("cannot find database named: " + startRequest.getDatabaseName()).
                asRuntimeException();
            synchronized (responseObserver) {
              responseObserver.onError(toThrow);
            }
            throw toThrow;
          }
          tx = db.createTransaction();
          setDeadline(rpcContext, tx);
          if (overallSpan != null) {
            overallSpan.tag("client", startRequest.getClientIdentifier()).
                tag("database_name", startRequest.getDatabaseName()).
                tag("name", startRequest.getName());
          }
        } else if (value.hasCommitTransaction()) {
          if (logger.isDebugEnabled()) {
            if (overallSpan != null) {
              overallSpan.event("CommitTransactionRequest");
            }
            logger.debug("CommitTransactionRequest");
          }
          hasActiveTransactionOrThrow();
          if (commitStarted.getAndSet(true)) {
            StatusRuntimeException toThrow = Status.INVALID_ARGUMENT.
                withDescription("transaction already committed").
                asRuntimeException();
            responseObserver.onError(toThrow);
            throw toThrow;
          }
          if (overallSpan != null) {
            overallSpan.tag("commit", "true");
          }
          // start the span and scope for the commit transaction call.
          Span opSpan = tracer.nextSpan(overallSpan).name("execute_transaction.commit_transaction");
          Tracer.SpanInScope opScope = tracer.withSpan(opSpan.start());
          handleException(tx.commit(), opSpan, responseObserver, "failed to commit transaction").
              thenAccept(x -> {
                try (Tracer.SpanInScope ignored = tracer.withSpan(opSpan)) {
                  synchronized (responseObserver) {
                    responseObserver.onNext(StreamingDatabaseResponse.newBuilder().
                        setCommitTransaction(CommitTransactionResponse.newBuilder().
                            setCommittedVersion(tx.getCommittedVersion()).build()).build());
                  }
                  if (logger.isDebugEnabled()) {
                    String msg = "Committed transaction: " + tx.getCommittedVersion();
                    opSpan.event(msg);
                    logger.debug(msg);
                  }
                }
              }).whenComplete((unused, throwable) -> {
            opScope.close();
            opSpan.end();
          });
        } else if (value.hasGetValue()) {
          if (logger.isDebugEnabled()) {
            String msg = "GetValueRequest on: " + printable(value.getGetValue().getKey().toByteArray());
            if (overallSpan != null) {
              overallSpan.event(msg);
            }
            logger.debug(msg);
          }
          hasActiveTransactionOrThrow();
          // start the span/scope for the get_value call.
          Span opSpan = tracer.nextSpan(overallSpan).name("execute_transaction.get_value");
          Tracer.SpanInScope opScope = tracer.withSpan(opSpan.start());
          GetValueRequest getValueRequest = value.getGetValue();
          CompletableFuture<byte[]> getFuture = getValueRequest.getSnapshot() ?
              tx.snapshot().get(getValueRequest.getKey().toByteArray()) :
              tx.get(getValueRequest.getKey().toByteArray());
          getFuture.whenComplete((val, throwable) -> {
            try (Tracer.SpanInScope ignored = tracer.withSpan(opSpan)) {
              if (throwable != null) {
                handleThrowable(opSpan, throwable,
                    () -> "failed to get value for key: " + printable(getValueRequest.getKey().toByteArray()));
                OperationFailureResponse.Builder builder = OperationFailureResponse.newBuilder().
                    setSequenceId(value.getGetValue().getSequenceId()).
                    setMessage(throwable.getMessage());
                if (throwable instanceof FDBException) {
                  builder.setCode(((FDBException) throwable).getCode());
                }
                synchronized (responseObserver) {
                  responseObserver.onNext(StreamingDatabaseResponse.newBuilder().
                      setOperationFailure(builder.build()).
                      build());
                }
              } else {
                if (logger.isDebugEnabled()) {
                  String msg = "GetValueRequest on: " +
                      printable(value.getGetValue().getKey().toByteArray()) + " is: " +
                      printable(val);
                  logger.debug(msg);
                  opSpan.event(msg);
                }
                rowsRead.incrementAndGet();
                GetValueResponse.Builder build = GetValueResponse.newBuilder().
                    setSequenceId(value.getGetValue().getSequenceId());
                if (val != null) {
                  build.setValue(ByteString.copyFrom(val));
                }
                synchronized (responseObserver) {
                  responseObserver.onNext(StreamingDatabaseResponse.newBuilder().
                      setGetValue(build.build()).
                      build());
                }
              }
            } finally {
              opScope.close();
              opSpan.end();
            }
          });
        } else if (value.hasSetValue()) {
          hasActiveTransactionOrThrow();
          if (logger.isDebugEnabled()) {
            String msg = "SetValueRequest for: " +
                printable(value.getSetValue().getKey().toByteArray()) + " => " +
                printable(value.getSetValue().getValue().toByteArray());
            logger.debug(msg);
            overallSpan.event(msg);
          }
          rowsWritten.incrementAndGet();
          tx.set(value.getSetValue().getKey().toByteArray(),
              value.getSetValue().getValue().toByteArray());
        } else if (value.hasClearKey()) {
          hasActiveTransactionOrThrow();
          if (logger.isDebugEnabled()) {
            String msg = "ClearKeyRequest for: " +
                printable(value.getClearKey().getKey().toByteArray());
            logger.debug(msg);
            overallSpan.event(msg);
          }
          clears.incrementAndGet();
          tx.clear(value.getClearKey().getKey().toByteArray());
        } else if (value.hasClearRange()) {
          hasActiveTransactionOrThrow();
          if (logger.isDebugEnabled()) {
            String msg = "ClearKeyRangeRequest for: " +
                printable(value.getClearRange().getStart().toByteArray()) + " => " +
                printable(value.getClearRange().getEnd().toByteArray());
            logger.debug(msg);
            overallSpan.event(msg);
          }
          clears.incrementAndGet();
          tx.clear(value.getClearRange().getStart().toByteArray(),
              value.getClearRange().getEnd().toByteArray());
        } else if (value.hasGetRange()) {
          hasActiveTransactionOrThrow();
          rangeGets.incrementAndGet();
          // start the span/scope for the get_value call.
          Span opSpan = tracer.nextSpan(overallSpan).name("execute_transaction.get_range").start();
          try (Tracer.SpanInScope ignored = tracer.withSpan(opSpan.start())) {
            GetRangeRequest req = value.getGetRange();
            KeySelector start;
            if (req.hasStartBytes()) {
              start = new KeySelector(req.getStartBytes().toByteArray(), false, 1);
            } else if (req.hasStartKeySelector()) {
              start = new KeySelector(req.getStartKeySelector().getKey().toByteArray(),
                  req.getStartKeySelector().getOrEqual(), req.getStartKeySelector().getOffset());
            } else {
              OperationFailureResponse.Builder builder = OperationFailureResponse.newBuilder().
                  setSequenceId(value.getGetValue().getSequenceId()).
                  setMessage("must have start");
              synchronized (responseObserver) {
                responseObserver.onNext(StreamingDatabaseResponse.newBuilder().
                    setOperationFailure(builder.build()).
                    build());
              }
              throw Status.INVALID_ARGUMENT.withDescription("must have start").asRuntimeException();
            }
            KeySelector end;
            if (req.hasEndBytes()) {
              end = new KeySelector(req.getEndBytes().toByteArray(), false, 1);
            } else if (req.hasEndKeySelector()) {
              end = new KeySelector(req.getEndKeySelector().getKey().toByteArray(),
                  req.getEndKeySelector().getOrEqual(), req.getEndKeySelector().getOffset());
            } else {
              OperationFailureResponse.Builder builder = OperationFailureResponse.newBuilder().
                  setSequenceId(value.getGetValue().getSequenceId()).
                  setMessage("must have end");
              synchronized (responseObserver) {
                responseObserver.onNext(StreamingDatabaseResponse.newBuilder().
                    setOperationFailure(builder.build()).
                    build());
              }
              throw Status.INVALID_ARGUMENT.withDescription("must have end").asRuntimeException();
            }
            if (logger.isDebugEnabled()) {
              String msg = "GetRangeRequest from: " + start + " to: " + end + " reverse: " + req.getReverse() +
                  " limit: " + req.getLimit() + " mode: " + req.getStreamingMode();
              logger.debug(msg);
              overallSpan.event(msg);
            }
            StreamingMode mode = StreamingMode.ITERATOR;
            switch (req.getStreamingMode()) {
              case ITERATOR:
                mode = StreamingMode.ITERATOR;
                break;
              case WANT_ALL:
                mode = StreamingMode.WANT_ALL;
                break;
              case EXACT:
                mode = StreamingMode.EXACT;
                break;
              case UNRECOGNIZED:
                mode = StreamingMode.ITERATOR;
                break;
            }
            AsyncIterable<KeyValue> range = req.getSnapshot() ?
                tx.snapshot().getRange(start, end, req.getLimit(), req.getReverse(), mode) :
                tx.getRange(start, end, req.getLimit(), req.getReverse(), mode);
            AsyncIterator<KeyValue> iterator = range.iterator();
            // consumer that collects key values and returns them to the user.
            AtomicLong rows = new AtomicLong();
            AtomicLong batches = new AtomicLong();
            BiConsumer<Boolean, Throwable> hasNextConsumer = new BiConsumer<>() {

              private final List<io.github.panghy.lionrock.proto.KeyValue> keyValues = new ArrayList<>();

              @Override
              public void accept(Boolean hasNext, Throwable throwable) {
                try (Tracer.SpanInScope ignored = tracer.withSpan(opSpan)) {
                  boolean done = false;
                  if (throwable != null) {
                    handleThrowable(opSpan, throwable,
                        () -> "failed to get range for start: " + start + " end: " + end +
                            " reverse: " + req.getReverse() + " limit: " + req.getLimit() +
                            " mode: " + req.getStreamingMode());
                    OperationFailureResponse.Builder builder = OperationFailureResponse.newBuilder().
                        setSequenceId(value.getGetRange().getSequenceId()).
                        setMessage(throwable.getMessage());
                    if (throwable instanceof FDBException) {
                      builder.setCode(((FDBException) throwable).getCode());
                    }
                    opSpan.tag("rows_read", String.valueOf(rows.get()));
                    opSpan.tag("batches", String.valueOf(batches.get()));
                    opSpan.end();
                    synchronized (responseObserver) {
                      responseObserver.onNext(StreamingDatabaseResponse.newBuilder().
                          setOperationFailure(builder.build()).
                          build());
                    }
                    return;
                  } else if (!hasNext) {
                    // no more rows to read, flush the last message.
                    done = true;
                  } else {
                    // spool everything until the onHasNext CompletableFuture is pending.
                    while (iterator.onHasNext().isDone() &&
                        !iterator.onHasNext().isCompletedExceptionally()) {
                      if (!iterator.hasNext()) {
                        done = true;
                        break;
                      }
                      KeyValue next = iterator.next();
                      rows.incrementAndGet();
                      synchronized (keyValues) {
                        keyValues.add(io.github.panghy.lionrock.proto.KeyValue.newBuilder().
                            setKey(ByteString.copyFrom(next.getKey())).
                            setValue(ByteString.copyFrom(next.getValue())).
                            build());
                      }
                    }
                  }
                  // flush what we have.
                  flush(done);
                  if (done) {
                    opSpan.tag("rows_read", String.valueOf(rows.get()));
                    opSpan.tag("batches", String.valueOf(batches.get()));
                    opSpan.end();
                  } else {
                    // schedule the callback on when the future is ready.
                    iterator.onHasNext().whenComplete(this);
                  }
                }
              }

              private void flush(boolean done) {
                if (!done && keyValues.isEmpty()) {
                  return;
                }
                batches.incrementAndGet();
                rangeGetBatches.incrementAndGet();
                rowsRead.addAndGet(keyValues.size());
                if (logger.isDebugEnabled()) {
                  String msg = "Flushing: " + keyValues.size() + " rows, done: " + done;
                  logger.debug(msg);
                  opSpan.event(msg);
                }
                synchronized (responseObserver) {
                  responseObserver.onNext(StreamingDatabaseResponse.newBuilder().
                      setGetRange(GetRangeResponse.newBuilder().
                          setDone(done).
                          addAllKeyValues(keyValues).
                          setSequenceId(req.getSequenceId()).
                          build()).
                      build());
                }
                keyValues.clear();
              }
            };
            iterator.onHasNext().whenComplete(hasNextConsumer);
          }
        } else if (value.hasAddConflictKey()) {
          hasActiveTransactionOrThrow();
          if (logger.isDebugEnabled()) {
            String msg = "AddConflictKeyRequest for: " + printable(value.getAddConflictKey().getKey().toByteArray()) +
                " write: " + value.getAddConflictKey().getWrite();
            logger.debug(msg);
            if (overallSpan != null) {
              overallSpan.event(msg);
            }
          }
          if (value.getAddConflictKey().getWrite()) {
            writeConflictAdds.incrementAndGet();
            tx.addWriteConflictKey(value.getAddConflictKey().getKey().toByteArray());
          } else {
            readConflictAdds.incrementAndGet();
            tx.addReadConflictKey(value.getAddConflictKey().getKey().toByteArray());
          }
        } else if (value.hasAddConflictRange()) {
          hasActiveTransactionOrThrow();
          if (logger.isDebugEnabled()) {
            String msg = "AddConflictRangeRequest from: " +
                printable(value.getAddConflictRange().getStart().toByteArray()) + " to: " +
                printable(value.getAddConflictRange().getEnd().toByteArray()) +
                " write: " + value.getAddConflictRange().getWrite();
            logger.debug(msg);
            if (overallSpan != null) {
              overallSpan.event(msg);
            }
          }
          if (value.getAddConflictRange().getWrite()) {
            writeConflictAdds.incrementAndGet();
            tx.addWriteConflictRange(value.getAddConflictRange().getStart().toByteArray(),
                value.getAddConflictRange().getEnd().toByteArray());
          } else {
            readConflictAdds.incrementAndGet();
            tx.addReadConflictRange(value.getAddConflictRange().getStart().toByteArray(),
                value.getAddConflictRange().getEnd().toByteArray());
          }
        } else if (value.hasGetReadVersion()) {
          hasActiveTransactionOrThrow();
          if (logger.isDebugEnabled()) {
            logger.debug("GetReadVersion");
            if (overallSpan != null) {
              overallSpan.event("GetReadVersion");
            }
          }
          // start the span/scope for the get_value call.
          Span opSpan = tracer.nextSpan(overallSpan).name("execute_transaction.get_read_version");
          Tracer.SpanInScope opScope = tracer.withSpan(opSpan.start());
          tx.getReadVersion().whenComplete((val, throwable) -> {
            try (Tracer.SpanInScope ignored = tracer.withSpan(opSpan)) {
              if (throwable != null) {
                handleThrowable(opSpan, throwable, () -> "failed to get read version");
                OperationFailureResponse.Builder builder = OperationFailureResponse.newBuilder().
                    setSequenceId(value.getGetReadVersion().getSequenceId()).
                    setMessage(throwable.getMessage());
                if (throwable instanceof FDBException) {
                  builder.setCode(((FDBException) throwable).getCode());
                }
                synchronized (responseObserver) {
                  responseObserver.onNext(StreamingDatabaseResponse.newBuilder().
                      setOperationFailure(builder.build()).
                      build());
                }
              } else {
                if (logger.isDebugEnabled()) {
                  String msg = "GetReadVersion is: " + val;
                  logger.debug(msg);
                  opSpan.event(msg);
                }
                getReadVersion.incrementAndGet();
                responseObserver.onNext(StreamingDatabaseResponse.newBuilder().
                    setGetReadVersion(GetReadVersionResponse.newBuilder().setReadVersion(val).build()).build());
              }
            } finally {
              opScope.close();
              opSpan.end();
            }
          });
        } else if (value.hasSetReadVersion()) {
          hasActiveTransactionOrThrow();
          if (logger.isDebugEnabled()) {
            String msg = "SetReadVersion at: " + value.getSetReadVersion().getReadVersion();
            logger.debug(msg);
            if (overallSpan != null) {
              overallSpan.event(msg);
            }
          }
          tx.setReadVersion(value.getSetReadVersion().getReadVersion());
        } else if (value.hasSetTransactionOption()) {
          hasActiveTransactionOrThrow();
          if (logger.isDebugEnabled()) {
            String msg = "SetTransactionOption: " + value.getSetTransactionOption().getOption() + " with: " +
                (value.getSetTransactionOption().hasParam() ?
                    printable(value.getSetTransactionOption().getParam().toByteArray()) : null);
            logger.debug(msg);
            if (overallSpan != null) {
              overallSpan.event(msg);
            }
          }
          if (value.getSetTransactionOption().hasParam()) {
            tx.options().getOptionConsumer().setOption(
                value.getSetTransactionOption().getOption(),
                value.getSetTransactionOption().getParam().toByteArray());
          } else {
            tx.options().getOptionConsumer().setOption(
                value.getSetTransactionOption().getOption(), null);
          }
        } else if (value.hasMutateValue()) {
          hasActiveTransactionOrThrow();
          if (logger.isDebugEnabled()) {
            String msg = "MutateValueRequest for: " +
                printable(value.getMutateValue().getKey().toByteArray()) + " => " +
                printable(value.getMutateValue().getParam().toByteArray()) + " with: " +
                value.getMutateValue().getType();
            logger.debug(msg);
            if (overallSpan != null) {
              overallSpan.event(msg);
            }
          }
          rowsMutated.incrementAndGet();
          tx.mutate(getMutationType(value.getMutateValue().getType()),
              value.getMutateValue().getKey().toByteArray(),
              value.getMutateValue().getParam().toByteArray());
        } else if (value.hasGetVersionstamp()) {
          hasActiveTransactionOrThrow();
          if (logger.isDebugEnabled()) {
            String msg = "GetVersionstampRequest";
            logger.debug(msg);
            if (overallSpan != null) {
              overallSpan.event(msg);
            }
          }// start the span/scope for the get_value call.
          Span opSpan = tracer.nextSpan(overallSpan).name("execute_transaction.get_versionstamp");
          Tracer.SpanInScope opScope = tracer.withSpan(opSpan.start());
          getVersionstamp.incrementAndGet();
          chainPostCommitFuture(tx.getVersionstamp().whenComplete((vs, throwable) -> {
            try (Tracer.SpanInScope ignored = tracer.withSpan(opSpan)) {
              if (throwable != null) {
                handleThrowable(opSpan, throwable, () -> "failed to get versionstamp");
                OperationFailureResponse.Builder builder = OperationFailureResponse.newBuilder().
                    setSequenceId(value.getGetVersionstamp().getSequenceId()).
                    setMessage(throwable.getMessage());
                if (throwable instanceof FDBException) {
                  builder.setCode(((FDBException) throwable).getCode());
                }
                synchronized (responseObserver) {
                  responseObserver.onNext(StreamingDatabaseResponse.newBuilder().
                      setOperationFailure(builder.build()).
                      build());
                }
              } else {
                if (logger.isDebugEnabled()) {
                  String msg = "GetVersionstampRequest is: " + printable(vs);
                  logger.debug(msg);
                  opSpan.event(msg);
                }
                synchronized (responseObserver) {
                  responseObserver.onNext(StreamingDatabaseResponse.newBuilder().
                      setGetVersionstamp(GetVersionstampResponse.newBuilder().
                          setSequenceId(value.getGetVersionstamp().getSequenceId()).
                          setVersionstamp(ByteString.copyFrom(vs)).build()).
                      build());
                }
              }
            } finally {
              opScope.close();
              opSpan.end();
            }
          }));
        } else if (value.hasWatchKey()) {
          hasActiveTransactionOrThrow();
          if (logger.isDebugEnabled()) {
            String msg = "WatchKeyRequest for: " + printable(value.getWatchKey().getKey().toByteArray());
            logger.debug(msg);
            if (overallSpan != null) {
              overallSpan.event(msg);
            }
          }// start the span/scope for the get_value call.
          Span opSpan = tracer.nextSpan(overallSpan).name("execute_transaction.watch_key");
          Tracer.SpanInScope opScope = tracer.withSpan(opSpan.start());
          getVersionstamp.incrementAndGet();
          chainPostCommitFuture(tx.watch(value.getWatchKey().getKey().toByteArray()).
              whenComplete((vs, throwable) -> {
                try (Tracer.SpanInScope ignored = tracer.withSpan(opSpan)) {
                  if (throwable != null) {
                    handleThrowable(opSpan, throwable, () -> "failed to watch key");
                    OperationFailureResponse.Builder builder = OperationFailureResponse.newBuilder().
                        setSequenceId(value.getWatchKey().getSequenceId()).
                        setMessage(throwable.getMessage());
                    if (throwable instanceof FDBException) {
                      builder.setCode(((FDBException) throwable).getCode());
                    }
                    synchronized (responseObserver) {
                      responseObserver.onNext(StreamingDatabaseResponse.newBuilder().
                          setOperationFailure(builder.build()).
                          build());
                    }
                  } else {
                    if (logger.isDebugEnabled()) {
                      String msg = "WatchKeyRequest Completed for: " +
                          printable(value.getWatchKey().getKey().toByteArray());
                      logger.debug(msg);
                      opSpan.event(msg);
                    }
                    synchronized (responseObserver) {
                      responseObserver.onNext(StreamingDatabaseResponse.newBuilder().
                          setWatchKey(WatchKeyResponse.newBuilder().
                              setSequenceId(value.getWatchKey().getSequenceId()).build()).
                          build());
                    }
                  }
                } finally {
                  opScope.close();
                  opSpan.end();
                }
              }));
        }
      }

      private void chainPostCommitFuture(CompletableFuture<?> postCommitFuture) {
        // we must reference the existing future here (and not in the lambda).
        CompletableFuture<?> existingFuture = this.postCommitFutures;
        synchronized (this) {
          // eat the exception from this future and compose it with the existing chain.
          this.postCommitFutures = postCommitFuture.
              exceptionally(x -> null).
              thenCompose(x -> existingFuture);
        }
      }

      private void hasActiveTransactionOrThrow() {
        if (tx == null && !commitStarted.get()) {
          StatusRuntimeException toThrow = Status.INVALID_ARGUMENT.
              withDescription("must have an active transaction").
              asRuntimeException();
          synchronized (responseObserver) {
            responseObserver.onError(toThrow);
          }
          throw toThrow;
        }
      }

      @Override
      public void onError(Throwable t) {
        logger.warn("onError from client in executeTransaction", t);
        if (tx != null) {
          tx.cancel();
          tx.close();
        }
      }

      @Override
      public void onCompleted() {
        // we only close the server-side connection when all post commit futures are resolved (whether success or
        // failures).
        postCommitFutures.whenComplete((ignored, throwable) -> {
          if (tx != null) {
            if (!commitStarted.get()) {
              tx.cancel();
            }
            tx.close();
          }
          if (overallSpan != null) {
            if (rowsRead.get() > 0) {
              overallSpan.tag("rows_read", String.valueOf(rowsRead.get()));
            }
            if (rangeGetBatches.get() > 0) {
              overallSpan.tag("range_get.batches", String.valueOf(rangeGetBatches.get()));
            }
            if (rowsWritten.get() > 0) {
              overallSpan.tag("rows_written", String.valueOf(rowsWritten.get()));
            }
            if (clears.get() > 0) {
              overallSpan.tag("clears", String.valueOf(clears.get()));
            }
            if (getReadVersion.get() > 0) {
              overallSpan.tag("read_version.gets", String.valueOf(getReadVersion.get()));
            }
            if (readConflictAdds.get() > 0) {
              overallSpan.tag("add_read_conflicts", String.valueOf(readConflictAdds.get()));
            }
            if (writeConflictAdds.get() > 0) {
              overallSpan.tag("add_write_conflicts", String.valueOf(writeConflictAdds.get()));
            }
            if (rowsMutated.get() > 0) {
              overallSpan.tag("rows_mutated", String.valueOf(rowsMutated.get()));
            }
            if (getVersionstamp.get() > 0) {
              overallSpan.tag("get_versionstamp", String.valueOf(getVersionstamp.get()));
            }
          }
          // close the connection after all post commits are done.
          try {
            synchronized (this) {
              responseObserver.onCompleted();
            }
          } catch (IllegalStateException ex) {
            // ignored.
          }
        });
      }
    };
  }

  private com.apple.foundationdb.MutationType getMutationType(MutationType mutationType) {
    switch (mutationType) {
      case ADD:
        return com.apple.foundationdb.MutationType.ADD;
      case BIT_AND:
        return com.apple.foundationdb.MutationType.BIT_AND;
      case BIT_OR:
        return com.apple.foundationdb.MutationType.BIT_OR;
      case BIT_XOR:
        return com.apple.foundationdb.MutationType.BIT_XOR;
      case APPEND_IF_FITS:
        return com.apple.foundationdb.MutationType.APPEND_IF_FITS;
      case MAX:
        return com.apple.foundationdb.MutationType.MAX;
      case MIN:
        return com.apple.foundationdb.MutationType.MIN;
      case SET_VERSIONSTAMPED_KEY:
        return com.apple.foundationdb.MutationType.SET_VERSIONSTAMPED_KEY;
      case SET_VERSIONSTAMPED_VALUE:
        return com.apple.foundationdb.MutationType.SET_VERSIONSTAMPED_VALUE;
      case BYTE_MIN:
        return com.apple.foundationdb.MutationType.BYTE_MIN;
      case BYTE_MAX:
        return com.apple.foundationdb.MutationType.BYTE_MAX;
      case COMPARE_AND_CLEAR:
        return com.apple.foundationdb.MutationType.COMPARE_AND_CLEAR;
      case UNRECOGNIZED:
      default:
        throw Status.INVALID_ARGUMENT.withDescription("unknown mutation type: " + mutationType).
            asRuntimeException();
    }
  }

  private void prepareTx(DatabaseRequest databaseRequest, Context rpcContext, Transaction tx) {
    setDeadline(rpcContext, tx);
    if (databaseRequest.getReadVersion() > 0) {
      tx.setReadVersion(databaseRequest.getReadVersion());
    }
    if (databaseRequest.getTransactionOptionsCount() > 0) {
      for (SetTransactionOptionRequest setTransactionOptionRequest : databaseRequest.getTransactionOptionsList()) {
        if (setTransactionOptionRequest.hasParam()) {
          tx.options().getOptionConsumer().setOption(setTransactionOptionRequest.getOption(),
              setTransactionOptionRequest.getParam().toByteArray());
        } else {
          tx.options().getOptionConsumer().setOption(setTransactionOptionRequest.getOption(), null);
        }
      }
    }
  }

  private void setDeadline(Context rpcContext, Transaction tx) {
    if (rpcContext.getDeadline() != null) {
      long value = rpcContext.getDeadline().timeRemaining(TimeUnit.MILLISECONDS);
      if (value <= 0) {
        throw Status.DEADLINE_EXCEEDED.asRuntimeException();
      }
      tx.options().setTimeout(value);
    } else {
      tx.options().setTimeout(config.getDefaultFdbTimeoutMs());
    }
  }

  private void handleCommittedTransaction(CompletableFuture<Transaction> input,
                                          StreamObserver<DatabaseResponse> responseObserver) {
    input.thenAccept(x -> {
      synchronized (responseObserver) {
        responseObserver.onNext(DatabaseResponse.newBuilder().
            setCommittedTransaction(CommitTransactionResponse.newBuilder().build()).
            build());
        responseObserver.onCompleted();
      }
    });
  }

  private Metadata handleThrowable(Span span, Throwable throwable, Supplier<String> failureMessage) {
    Metadata exceptionMetadata = new Metadata();
    if (throwable instanceof FDBException) {
      span.tag("fdb_error_code", String.valueOf(((FDBException) throwable).getCode())).
          error(throwable);
      span.tag("fdb_error_message", throwable.getMessage());
      exceptionMetadata.put(Metadata.Key.of("fdb_error_code", Metadata.ASCII_STRING_MARSHALLER),
          String.valueOf(((FDBException) throwable).getCode()));
      exceptionMetadata.put(Metadata.Key.of("fdb_error_message", Metadata.ASCII_STRING_MARSHALLER),
          throwable.getMessage());
    }
    span.event(failureMessage.get());
    logger.warn(failureMessage.get(), throwable);
    span.error(throwable);
    return exceptionMetadata;
  }

  private <T> CompletableFuture<T> handleException(CompletableFuture<T> input, Span overallSpan,
                                                   StreamObserver<?> responseObserver,
                                                   String failureMessage) {
    return handleException(input, overallSpan, responseObserver, () -> failureMessage);
  }

  /**
   * Handle exceptional cases for a {@link CompletableFuture} and emit an error to the {@link StreamObserver} as well
   * as recording a span.
   */
  private <T> CompletableFuture<T> handleException(CompletableFuture<T> input, Span overallSpan,
                                                   StreamObserver<?> responseObserver,
                                                   Supplier<String> failureMessage) {
    input.whenComplete((t, throwable) -> {
      if (throwable != null) {
        try (Tracer.SpanInScope ignored = tracer.withSpan(overallSpan)) {
          Metadata exceptionMetadata = handleThrowable(overallSpan, throwable, failureMessage);
          synchronized (responseObserver) {
            responseObserver.onError(Status.ABORTED.
                withCause(throwable).
                withDescription(throwable.getMessage()).
                asRuntimeException(exceptionMetadata));
          }
        }
      }
    });
    return input;
  }

  public static void main(String[] args) {
    SpringApplication.run(FoundationDbGrpcFacade.class, args);
  }
}
