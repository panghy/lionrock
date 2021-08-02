package io.github.panghy.lionrock.foundationdb;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.CloseableAsyncIterator;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
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
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.apple.foundationdb.tuple.ByteArrayUtil.printable;
import static java.util.concurrent.CompletableFuture.completedFuture;

@SpringBootApplication
@GRpcService
@Component
public class FoundationDbGrpcFacade extends TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreImplBase {

  Logger logger = LoggerFactory.getLogger(FoundationDbGrpcFacade.class);

  @Autowired
  Configuration config;
  @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
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
      synchronized (responseObserver) {
        responseObserver.onError(toThrow);
      }
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
          synchronized (responseObserver) {
            responseObserver.onNext(DatabaseResponse.newBuilder().
                setGetValue(build.build()).
                build());
            responseObserver.onCompleted();
          }
        }
      });
    } else if (request.hasGetKey()) {
      if (overallSpan != null) {
        overallSpan.tag("request", "get_key");
      }
      io.github.panghy.lionrock.proto.KeySelector ks = request.getGetKey().getKeySelector();
      KeySelector keySelector = new KeySelector(ks.getKey().toByteArray(), ks.getOrEqual(), ks.getOffset());
      if (logger.isDebugEnabled()) {
        String msg = "GetKey for: " + keySelector;
        logger.debug(msg);
        if (overallSpan != null) {
          overallSpan.event(msg);
        }
      }
      handleException(db.runAsync(tx -> {
        prepareTx(request, rpcContext, tx);
        return tx.getKey(keySelector);
      }), overallSpan, responseObserver, "failed to get key").thenAccept(val -> {
        try (Tracer.SpanInScope ignored = tracer.withSpan(overallSpan)) {
          GetKeyResponse.Builder build = GetKeyResponse.newBuilder();
          if (logger.isDebugEnabled()) {
            String msg = "GetKey on: " +
                keySelector + " is: " + printable(val);
            logger.debug(msg);
            if (overallSpan != null) {
              overallSpan.event(msg);
            }
          }
          if (val != null) {
            build.setKey(ByteString.copyFrom(val));
          }
          synchronized (responseObserver) {
            responseObserver.onNext(DatabaseResponse.newBuilder().
                setGetKey(build.build()).
                build());
            responseObserver.onCompleted();
          }
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
        if (overallSpan != null) {
          overallSpan.event(msg);
        }
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
        case WANT_ALL:
          mode = StreamingMode.WANT_ALL;
          break;
        case EXACT:
          mode = StreamingMode.EXACT;
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
          synchronized (responseObserver) {
            responseObserver.onNext(DatabaseResponse.newBuilder().
                setGetRange(build.build()).
                build());
            responseObserver.onCompleted();
          }
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
    } else if (request.hasGetEstimatedRangeSize()) {
      if (overallSpan != null) {
        overallSpan.tag("request", "get_estimated_range_size");
      }
      byte[] startB = request.getGetEstimatedRangeSize().getStart().toByteArray();
      byte[] endB = request.getGetEstimatedRangeSize().getEnd().toByteArray();
      if (logger.isDebugEnabled()) {
        String msg = "GetEstimatedRangeSize for start: " + printable(startB) + " end: " + printable(endB);
        logger.debug(msg);
        if (overallSpan != null) {
          overallSpan.event(msg);
        }
      }
      handleException(db.runAsync(tx -> {
        prepareTx(request, rpcContext, tx);
        return tx.getEstimatedRangeSizeBytes(startB, endB);
      }), overallSpan, responseObserver, "failed to get estimated range size").thenAccept(val -> {
        try (Tracer.SpanInScope ignored = tracer.withSpan(overallSpan)) {
          GetEstimatedRangeSizeResponse.Builder build = GetEstimatedRangeSizeResponse.newBuilder();
          if (logger.isDebugEnabled()) {
            String msg = "GetEstimatedRangeSize for start: " + printable(startB) + " end: " + printable(endB) +
                "is: " + val;
            logger.debug(msg);
            if (overallSpan != null) {
              overallSpan.event(msg);
            }
          }
          if (val != null) {
            build.setSize(val);
          }
          synchronized (responseObserver) {
            responseObserver.onNext(DatabaseResponse.newBuilder().
                setGetEstimatedRangeSize(build.build()).
                build());
            responseObserver.onCompleted();
          }
        }
      });
    } else if (request.hasGetBoundaryKeys()) {
      if (overallSpan != null) {
        overallSpan.tag("request", "get_boundary_keys");
      }
      GetBoundaryKeysRequest req = request.getGetBoundaryKeys();
      byte[] startB = req.getStart().toByteArray();
      byte[] endB = req.getEnd().toByteArray();
      if (logger.isDebugEnabled()) {
        String msg = "GetBoundaryKeysRequest from: " + printable(startB) + " to: " + printable(endB);
        logger.debug(msg);
        if (overallSpan != null) {
          overallSpan.event(msg);
        }
      }
      handleException(db.runAsync(tx -> {
            prepareTx(request, rpcContext, tx);
            if (request.getReadVersion() > 0) {
              tx.setReadVersion(request.getReadVersion());
            }
            CloseableAsyncIterator<byte[]> boundaryKeys = LocalityUtil.getBoundaryKeys(tx, startB, endB);
            return AsyncUtil.collectRemaining(boundaryKeys).
                whenComplete((bytes, throwable) -> boundaryKeys.close());
          }),
          overallSpan, responseObserver, "failed to get boundary keys").thenAccept(results -> {
        try (Tracer.SpanInScope ignored = tracer.withSpan(overallSpan)) {
          GetBoundaryKeysResponse.Builder build = GetBoundaryKeysResponse.newBuilder();
          if (logger.isDebugEnabled()) {
            String msg = "GetBoundaryKeysRequest from: " + printable(startB) + " to: " + printable(endB) +
                " got: " + results.size() + " rows";
            logger.debug(msg);
            if (overallSpan != null) {
              overallSpan.event(msg);
            }
          }
          if (overallSpan != null) {
            overallSpan.tag("rows", String.valueOf(results.size()));
          }
          build.setDone(true);
          build.addAllKeys(results.stream().map(ByteString::copyFrom).collect(Collectors.toList()));
          synchronized (responseObserver) {
            responseObserver.onNext(DatabaseResponse.newBuilder().
                setGetBoundaryKeys(build.build()).
                build());
            responseObserver.onCompleted();
          }
        }
      });
    } else if (request.hasGetAddressesForKey()) {
      if (overallSpan != null) {
        overallSpan.tag("request", "get_addresses_for_key");
      }
      handleException(db.runAsync(tx -> {
        prepareTx(request, rpcContext, tx);
        return LocalityUtil.getAddressesForKey(tx, request.getGetAddressesForKey().getKey().toByteArray());
      }), overallSpan, responseObserver, "failed to get key").thenAccept(val -> {
        try (Tracer.SpanInScope ignored = tracer.withSpan(overallSpan)) {
          GetAddressesForKeyResponse.Builder build = GetAddressesForKeyResponse.newBuilder();
          if (logger.isDebugEnabled()) {
            String msg = "GetAddressesForKey for: " +
                printable(request.getGetAddressesForKey().getKey().toByteArray()) + " is: " + Joiner.on(",").join(val);
            logger.debug(msg);
            if (overallSpan != null) {
              overallSpan.event(msg);
            }
          }
          if (val != null) {
            build.addAllAddresses(Arrays.stream(val).collect(Collectors.toList()));
          }
          synchronized (responseObserver) {
            responseObserver.onNext(DatabaseResponse.newBuilder().
                setGetAddresssesForKey(build).
                build());
            responseObserver.onCompleted();
          }
        }
      });
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
      private final AtomicLong keysRead = new AtomicLong();
      private final AtomicLong getReadVersion = new AtomicLong();
      private final AtomicLong rangeGets = new AtomicLong();
      private final AtomicLong rangeGetBatches = new AtomicLong();
      private final AtomicLong clears = new AtomicLong();
      private final AtomicLong readConflictAdds = new AtomicLong();
      private final AtomicLong writeConflictAdds = new AtomicLong();
      private final AtomicLong getVersionstamp = new AtomicLong();
      private final AtomicLong getApproximateSize = new AtomicLong();
      private final AtomicLong getEstimatedRangeSize = new AtomicLong();
      private final AtomicLong getBoundaryKeys = new AtomicLong();
      private final AtomicLong getAddressesForKey = new AtomicLong();

      private final Set<Long> knownSequenceIds = Collections.newSetFromMap(new ConcurrentHashMap<>());

      private volatile Transaction tx;
      /**
       * Long-living futures that might last beyond the open->commit() lifecycle of a transaction(e.g. getVersionStamp
       * and watch).
       */
      private final List<CompletableFuture<?>> longLivingFutures = new ArrayList<>();

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
                if (tx != null) {
                  tx.close();
                }
              }
              throw toThrow;
            }
            return value.getStartTransaction();
          });
          if (logger.isDebugEnabled()) {
            String msg = "Starting transaction " + startRequest.getName() + " against db: " +
                startRequest.getDatabaseName();
            logger.debug(msg);
            if (overallSpan != null) {
              overallSpan.event(msg);
            }
          }
          Database db = databaseMap.get(startRequest.getDatabaseName());
          if (db == null) {
            StatusRuntimeException toThrow = Status.INVALID_ARGUMENT.
                withDescription("cannot find database named: " + startRequest.getDatabaseName()).
                asRuntimeException();
            synchronized (responseObserver) {
              responseObserver.onError(toThrow);
              if (tx != null) {
                tx.close();
              }
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
          hasActiveTransactionOrThrow();
          if (logger.isDebugEnabled()) {
            if (overallSpan != null) {
              overallSpan.event("CommitTransactionRequest");
            }
            logger.debug("CommitTransactionRequest");
          }
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
          CompletableFuture<byte[]> versionstampF = tx.getVersionstamp();
          handleException(tx.commit().thenCompose(x -> versionstampF.exceptionally(ex -> null)),
              opSpan, responseObserver,
              "failed to commit transaction").
              whenComplete((versionstamp, throwable) -> {
                try (Tracer.SpanInScope ignored = tracer.withSpan(opSpan)) {
                  if (throwable == null) {
                    synchronized (responseObserver) {
                      CommitTransactionResponse.Builder builder = CommitTransactionResponse.newBuilder().
                          setCommittedVersion(tx.getCommittedVersion());
                      if (versionstamp != null) {
                        builder.setVersionstamp(ByteString.copyFrom(versionstamp));
                      }
                      responseObserver.onNext(StreamingDatabaseResponse.newBuilder().
                          setCommitTransaction(builder.build()).
                          build());
                    }
                    if (logger.isDebugEnabled()) {
                      String msg = "Committed transaction: " + tx.getCommittedVersion();
                      opSpan.event(msg);
                      logger.debug(msg);
                    }
                    // terminate the connection to the client when all long living futures are done.
                    CompletableFuture.allOf(longLivingFutures.toArray(CompletableFuture[]::new)).
                        whenComplete((val, y) -> {
                          logger.debug("server onCompleted()");
                          synchronized (responseObserver) {
                            responseObserver.onCompleted();
                          }
                          if (tx != null) {
                            tx.close();
                          }
                        });
                  } else {
                    // throwable != null
                    populateOverallSpanStats();
                    if (tx != null) {
                      tx.close();
                    }
                  }
                }
                opScope.close();
                opSpan.end();
              });
        } else if (value.hasGetValue()) {
          hasActiveTransactionOrThrow();
          GetValueRequest req = value.getGetValue();
          throwIfSequenceIdHasBeenSeen(req.getSequenceId());
          if (logger.isDebugEnabled()) {
            String msg = "GetValueRequest on: " + printable(value.getGetValue().getKey().toByteArray());
            if (overallSpan != null) {
              overallSpan.event(msg);
            }
            logger.debug(msg);
          }
          // start the span/scope for the get_value call.
          Span opSpan = tracer.nextSpan(overallSpan).name("execute_transaction.get_value");
          Tracer.SpanInScope opScope = tracer.withSpan(opSpan.start());
          CompletableFuture<byte[]> getFuture = req.getSnapshot() ?
              tx.snapshot().get(req.getKey().toByteArray()) :
              tx.get(req.getKey().toByteArray());
          getFuture.whenComplete((val, throwable) -> {
            try (Tracer.SpanInScope ignored = tracer.withSpan(opSpan)) {
              if (throwable != null) {
                handleThrowable(opSpan, throwable,
                    () -> "failed to get value for key: " + printable(req.getKey().toByteArray()));
                sendErrorToRemote(throwable, req.getSequenceId(), responseObserver);
              } else {
                if (logger.isDebugEnabled()) {
                  String msg = "GetValueRequest on: " +
                      printable(req.getKey().toByteArray()) + " is: " +
                      printable(val) + " seq_id: " + req.getSequenceId();
                  logger.debug(msg);
                  opSpan.event(msg);
                }
                rowsRead.incrementAndGet();
                GetValueResponse.Builder build = GetValueResponse.newBuilder().
                    setSequenceId(req.getSequenceId());
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
        } else if (value.hasGetKey()) {
          hasActiveTransactionOrThrow();
          GetKeyRequest req = value.getGetKey();
          throwIfSequenceIdHasBeenSeen(req.getSequenceId());
          io.github.panghy.lionrock.proto.KeySelector ks = req.getKeySelector();
          KeySelector keySelector = new KeySelector(ks.getKey().toByteArray(), ks.getOrEqual(), ks.getOffset());
          if (logger.isDebugEnabled()) {
            String msg = "GetKey for: " + keySelector;
            logger.debug(msg);
            if (overallSpan != null) {
              overallSpan.event(msg);
            }
          }
          // start the span/scope for the get_value call.
          Span opSpan = tracer.nextSpan(overallSpan).name("execute_transaction.get_key");
          Tracer.SpanInScope opScope = tracer.withSpan(opSpan.start());
          CompletableFuture<byte[]> getFuture = req.getSnapshot() ?
              tx.snapshot().getKey(keySelector) :
              tx.getKey(keySelector);
          getFuture.whenComplete((val, throwable) -> {
            try (Tracer.SpanInScope ignored = tracer.withSpan(opSpan)) {
              if (throwable != null) {
                handleThrowable(opSpan, throwable,
                    () -> "failed to get key: " + keySelector);
                sendErrorToRemote(throwable, req.getSequenceId(), responseObserver);
              } else {
                if (logger.isDebugEnabled()) {
                  String msg = "GetKey on: " +
                      keySelector + " is: " + printable(val);
                  logger.debug(msg);
                  opSpan.event(msg);
                }
                keysRead.incrementAndGet();
                GetKeyResponse.Builder build = GetKeyResponse.newBuilder().
                    setSequenceId(req.getSequenceId());
                if (val != null) {
                  build.setKey(ByteString.copyFrom(val));
                }
                synchronized (responseObserver) {
                  responseObserver.onNext(StreamingDatabaseResponse.newBuilder().
                      setGetKey(build.build()).
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
          SetValueRequest req = value.getSetValue();
          setValue(req);
        } else if (value.hasClearKey()) {
          hasActiveTransactionOrThrow();
          ClearKeyRequest req = value.getClearKey();
          clearKey(req);
        } else if (value.hasClearRange()) {
          hasActiveTransactionOrThrow();
          ClearKeyRangeRequest req = value.getClearRange();
          clearRange(req);
        } else if (value.hasGetRange()) {
          hasActiveTransactionOrThrow();
          GetRangeRequest req = value.getGetRange();
          throwIfSequenceIdHasBeenSeen(req.getSequenceId());
          rangeGets.incrementAndGet();
          // start the span/scope for the get_value call.
          Span opSpan = tracer.nextSpan(overallSpan).name("execute_transaction.get_range").start();
          try (Tracer.SpanInScope ignored = tracer.withSpan(opSpan.start())) {
            KeySelector start;
            if (req.hasStartBytes()) {
              start = new KeySelector(req.getStartBytes().toByteArray(), false, 1);
            } else {
              start = new KeySelector(req.getStartKeySelector().getKey().toByteArray(),
                  req.getStartKeySelector().getOrEqual(), req.getStartKeySelector().getOffset());
            }
            KeySelector end;
            if (req.hasEndBytes()) {
              end = new KeySelector(req.getEndBytes().toByteArray(), false, 1);
            } else {
              end = new KeySelector(req.getEndKeySelector().getKey().toByteArray(),
                  req.getEndKeySelector().getOrEqual(), req.getEndKeySelector().getOffset());
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
              case WANT_ALL:
                mode = StreamingMode.WANT_ALL;
                break;
              case EXACT:
                mode = StreamingMode.EXACT;
                break;
            }
            AsyncIterable<KeyValue> range = req.getSnapshot() ?
                tx.snapshot().getRange(start, end, req.getLimit(), req.getReverse(), mode) :
                tx.getRange(start, end, req.getLimit(), req.getReverse(), mode);
            if (config.getInternal().isUseAsListForRangeGets()) {
              // asList() method.
              handleRangeGetWithAsList(req, opSpan, start, end, range);
            } else {
              // iterator method.
              handleRangeGetWithAsyncIterator(value, req, opSpan, start, end, range);
            }
          }
        } else if (value.hasAddConflictKey()) {
          hasActiveTransactionOrThrow();
          AddConflictKeyRequest req = value.getAddConflictKey();
          addConflictKey(req);
        } else if (value.hasAddConflictRange()) {
          hasActiveTransactionOrThrow();
          AddConflictRangeRequest req = value.getAddConflictRange();
          addConflictRange(req);
        } else if (value.hasGetReadVersion()) {
          hasActiveTransactionOrThrow();
          GetReadVersionRequest req = value.getGetReadVersion();
          throwIfSequenceIdHasBeenSeen(req.getSequenceId());
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
                sendErrorToRemote(throwable, req.getSequenceId(), responseObserver);
              } else {
                if (logger.isDebugEnabled()) {
                  String msg = "GetReadVersion is: " + val + " seq_id: " + req.getSequenceId();
                  logger.debug(msg);
                  opSpan.event(msg);
                }
                this.getReadVersion.incrementAndGet();
                synchronized (responseObserver) {
                  responseObserver.onNext(StreamingDatabaseResponse.newBuilder().
                      setGetReadVersion(GetReadVersionResponse.newBuilder().
                          setReadVersion(val).
                          setSequenceId(req.getSequenceId()).
                          build()).
                      build());
                }
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
          MutateValueRequest req = value.getMutateValue();
          mutateValue(req);
        } else if (value.hasWatchKey()) {
          hasActiveTransactionOrThrow();
          WatchKeyRequest req = value.getWatchKey();
          throwIfSequenceIdHasBeenSeen(req.getSequenceId());
          if (logger.isDebugEnabled()) {
            String msg = "WatchKeyRequest for: " + printable(req.getKey().toByteArray());
            logger.debug(msg);
            if (overallSpan != null) {
              overallSpan.event(msg);
            }
          }// start the span/scope for the get_value call.
          Span opSpan = tracer.nextSpan(overallSpan).name("execute_transaction.watch_key");
          Tracer.SpanInScope opScope = tracer.withSpan(opSpan.start());
          getVersionstamp.incrementAndGet();
          addLongLivingFuture(tx.watch(req.getKey().toByteArray()).
              whenComplete((vs, throwable) -> {
                try (Tracer.SpanInScope ignored = tracer.withSpan(opSpan)) {
                  if (throwable != null) {
                    handleThrowable(opSpan, throwable, () -> "failed to watch key");
                    sendErrorToRemote(throwable, req.getSequenceId(), responseObserver);
                  } else {
                    if (logger.isDebugEnabled()) {
                      String msg = "WatchKeyRequest Completed for: " +
                          printable(req.getKey().toByteArray());
                      logger.debug(msg);
                      opSpan.event(msg);
                    }
                    synchronized (responseObserver) {
                      responseObserver.onNext(StreamingDatabaseResponse.newBuilder().
                          setWatchKey(WatchKeyResponse.newBuilder().
                              setSequenceId(req.getSequenceId()).build()).
                          build());
                    }
                  }
                } finally {
                  opScope.close();
                  opSpan.end();
                }
              }));
        } else if (value.hasGetApproximateSize()) {
          hasActiveTransactionOrThrow();
          GetApproximateSizeRequest req = value.getGetApproximateSize();
          throwIfSequenceIdHasBeenSeen(req.getSequenceId());
          if (logger.isDebugEnabled()) {
            String msg = "GetApproximateSizeRequest";
            if (overallSpan != null) {
              overallSpan.event(msg);
            }
            logger.debug(msg);
          }
          // start the span/scope for the get_value call.
          Span opSpan = tracer.nextSpan(overallSpan).name("execute_transaction.get_approximate_size");
          Tracer.SpanInScope opScope = tracer.withSpan(opSpan.start());
          getApproximateSize.incrementAndGet();
          tx.getApproximateSize().whenComplete((val, throwable) -> {
            try (Tracer.SpanInScope ignored = tracer.withSpan(opSpan)) {
              if (throwable != null) {
                handleThrowable(opSpan, throwable,
                    () -> "failed to get approximate size");
                sendErrorToRemote(throwable, req.getSequenceId(), responseObserver);
              } else {
                if (logger.isDebugEnabled()) {
                  String msg = "GetApproximateSize is: " + val;
                  logger.debug(msg);
                  opSpan.event(msg);
                }
                GetApproximateSizeResponse.Builder build = GetApproximateSizeResponse.newBuilder().
                    setSequenceId(req.getSequenceId());
                if (val != null) {
                  build.setSize(val);
                }
                synchronized (responseObserver) {
                  responseObserver.onNext(StreamingDatabaseResponse.newBuilder().
                      setGetApproximateSize(build.build()).
                      build());
                }
              }
            } finally {
              opScope.close();
              opSpan.end();
            }
          });
        } else if (value.hasGetEstimatedRangeSize()) {
          hasActiveTransactionOrThrow();
          GetEstimatedRangeSizeRequest req = value.getGetEstimatedRangeSize();
          throwIfSequenceIdHasBeenSeen(req.getSequenceId());
          byte[] startB = req.getStart().toByteArray();
          byte[] endB = req.getEnd().toByteArray();
          if (logger.isDebugEnabled()) {
            String msg = "GetEstimatedRangeSize for start: " + printable(startB) + " end: " + printable(endB);
            if (overallSpan != null) {
              overallSpan.event(msg);
            }
            logger.debug(msg);
          }
          // start the span/scope for the get_value call.
          Span opSpan = tracer.nextSpan(overallSpan).name("execute_transaction.get_estimated_range_size");
          Tracer.SpanInScope opScope = tracer.withSpan(opSpan.start());
          this.getEstimatedRangeSize.incrementAndGet();
          tx.getEstimatedRangeSizeBytes(startB, endB).whenComplete((val, throwable) -> {
            try (Tracer.SpanInScope ignored = tracer.withSpan(opSpan)) {
              if (throwable != null) {
                handleThrowable(opSpan, throwable,
                    () -> "failed to get estimated range size");
                sendErrorToRemote(throwable, req.getSequenceId(), responseObserver);
              } else {
                if (logger.isDebugEnabled()) {
                  String msg = "GetEstimatedRangeSize for start: " + printable(startB) + " end: " + printable(endB) +
                      " is: " + val;
                  logger.debug(msg);
                  opSpan.event(msg);
                }
                GetEstimatedRangeSizeResponse.Builder build = GetEstimatedRangeSizeResponse.newBuilder().
                    setSequenceId(req.getSequenceId());
                if (val != null) {
                  build.setSize(val);
                }
                synchronized (responseObserver) {
                  responseObserver.onNext(StreamingDatabaseResponse.newBuilder().
                      setGetEstimatedRangeSize(build.build()).
                      build());
                }
              }
            } finally {
              opScope.close();
              opSpan.end();
            }
          });
        } else if (value.hasGetBoundaryKeys()) {
          hasActiveTransactionOrThrow();
          GetBoundaryKeysRequest req = value.getGetBoundaryKeys();
          throwIfSequenceIdHasBeenSeen(req.getSequenceId());
          getBoundaryKeys.incrementAndGet();
          // start the span/scope for the get_value call.
          Span opSpan = tracer.nextSpan(overallSpan).name("execute_transaction.get_boundary_keys").start();
          try (Tracer.SpanInScope ignored = tracer.withSpan(opSpan.start())) {
            byte[] startB = req.getStart().toByteArray();
            byte[] endB = req.getEnd().toByteArray();
            if (logger.isDebugEnabled()) {
              String msg = "GetBoundaryKeysRequest from: " + printable(startB) + " to: " + printable(endB);
              logger.debug(msg);
              if (overallSpan != null) {
                overallSpan.event(msg);
              }
            }
            CloseableAsyncIterator<byte[]> iterator = LocalityUtil.getBoundaryKeys(tx, startB, endB);
            // consumer that collects key values and returns them to the user.
            AtomicLong rows = new AtomicLong();
            AtomicLong batches = new AtomicLong();
            BiConsumer<Boolean, Throwable> hasNextConsumer = new BiConsumer<>() {

              private final List<ByteString> boundaries = new ArrayList<>();

              @Override
              public void accept(Boolean hasNext, Throwable throwable) {
                try (Tracer.SpanInScope ignored = tracer.withSpan(opSpan)) {
                  boolean done = false;
                  if (throwable != null) {
                    handleThrowable(opSpan, throwable,
                        () -> "failed to get boundary keys for start: " + printable(startB) +
                            " end: " + printable(endB));
                    OperationFailureResponse.Builder builder = OperationFailureResponse.newBuilder().
                        setSequenceId(value.getGetRange().getSequenceId()).
                        setMessage(throwable.getMessage());
                    if (throwable instanceof FDBException) {
                      builder.setCode(((FDBException) throwable).getCode());
                    }
                    iterator.close();
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
                      byte[] next = iterator.next();
                      rows.incrementAndGet();
                      synchronized (boundaries) {
                        boundaries.add(ByteString.copyFrom(next));
                      }
                    }
                  }
                  // flush what we have.
                  flush(done);
                  if (done) {
                    iterator.close();
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
                if (!done && boundaries.isEmpty()) {
                  return;
                }
                batches.incrementAndGet();
                rangeGetBatches.incrementAndGet();
                rowsRead.addAndGet(boundaries.size());
                if (logger.isDebugEnabled()) {
                  String msg = "GetBoundaryKeysRequest from: " + printable(startB) + " to: " + printable(endB) +
                      ", flushing: " + boundaries.size() + " boundaries, done: " + done;
                  logger.debug(msg);
                  opSpan.event(msg);
                }
                synchronized (responseObserver) {
                  responseObserver.onNext(StreamingDatabaseResponse.newBuilder().
                      setGetBoundaryKeys(GetBoundaryKeysResponse.newBuilder().
                          setDone(done).
                          addAllKeys(boundaries).
                          setSequenceId(req.getSequenceId()).
                          build()).
                      build());
                }
                boundaries.clear();
              }
            };
            iterator.onHasNext().whenComplete(hasNextConsumer);
          }
        } else if (value.hasGetAddressesForKey()) {
          hasActiveTransactionOrThrow();
          GetAddressesForKeyRequest req = value.getGetAddressesForKey();
          throwIfSequenceIdHasBeenSeen(req.getSequenceId());
          if (logger.isDebugEnabled()) {
            String msg = "GetAddressesForKey on: " + printable(value.getGetAddressesForKey().getKey().toByteArray());
            if (overallSpan != null) {
              overallSpan.event(msg);
            }
            logger.debug(msg);
          }
          getAddressesForKey.incrementAndGet();
          // start the span/scope for the get_value call.
          Span opSpan = tracer.nextSpan(overallSpan).name("execute_transaction.get_addresses_for_key");
          Tracer.SpanInScope opScope = tracer.withSpan(opSpan.start());
          CompletableFuture<String[]> getFuture = LocalityUtil.getAddressesForKey(tx, req.getKey().toByteArray());
          getFuture.whenComplete((val, throwable) -> {
            try (Tracer.SpanInScope ignored = tracer.withSpan(opSpan)) {
              if (throwable != null) {
                handleThrowable(opSpan, throwable,
                    () -> "failed to get addresses for key: " + printable(req.getKey().toByteArray()));
                sendErrorToRemote(throwable, req.getSequenceId(), responseObserver);
              } else {
                if (logger.isDebugEnabled()) {
                  String msg = "GetAddressesForKey on: " +
                      printable(req.getKey().toByteArray()) + " is: " +
                      Joiner.on(",").join(val);
                  logger.debug(msg);
                  opSpan.event(msg);
                }
                rowsRead.incrementAndGet();
                GetAddressesForKeyResponse.Builder build = GetAddressesForKeyResponse.newBuilder().
                    setSequenceId(req.getSequenceId());
                if (val != null) {
                  build.addAllAddresses(Arrays.asList(val));
                }
                synchronized (responseObserver) {
                  responseObserver.onNext(StreamingDatabaseResponse.newBuilder().
                      setGetAddressesForKey(build.build()).
                      build());
                }
              }
            } finally {
              opScope.close();
              opSpan.end();
            }
          });
        } else if (value.hasBatchedMutations()) {
          hasActiveTransactionOrThrow();
          List<BatchedMutations> mutations = value.getBatchedMutations().getMutationsList();
          mutations.forEach(mutation -> {
            if (mutation.hasSetValue()) {
              setValue(mutation.getSetValue());
            } else if (mutation.hasMutateValue()) {
              mutateValue(mutation.getMutateValue());
            } else if (mutation.hasClearKey()) {
              clearKey(mutation.getClearKey());
            } else if (mutation.hasClearRange()) {
              clearRange(mutation.getClearRange());
            } else if (mutation.hasAddConflictKey()) {
              addConflictKey(mutation.getAddConflictKey());
            } else if (mutation.hasAddConflictRange()) {
              addConflictRange(mutation.getAddConflictRange());
            }
          });
        }
      }

      private void setValue(SetValueRequest req) {
        if (logger.isDebugEnabled()) {
          String msg = "SetValueRequest for: " + printable(req.getKey().toByteArray()) + " => " +
              printable(req.getValue().toByteArray());
          logger.debug(msg);
          if (overallSpan != null) {
            overallSpan.event(msg);
          }
        }
        rowsWritten.incrementAndGet();
        tx.set(req.getKey().toByteArray(), req.getValue().toByteArray());
      }

      private void clearKey(ClearKeyRequest req) {
        if (logger.isDebugEnabled()) {
          String msg = "ClearKeyRequest for: " + printable(req.getKey().toByteArray());
          logger.debug(msg);
          if (overallSpan != null) {
            overallSpan.event(msg);
          }
        }
        clears.incrementAndGet();
        tx.clear(req.getKey().toByteArray());
      }

      private void clearRange(ClearKeyRangeRequest req) {
        if (logger.isDebugEnabled()) {
          String msg = "ClearKeyRangeRequest for: " +
              printable(req.getStart().toByteArray()) + " => " + printable(req.getEnd().toByteArray());
          logger.debug(msg);
          if (overallSpan != null) {
            overallSpan.event(msg);
          }
        }
        clears.incrementAndGet();
        tx.clear(req.getStart().toByteArray(), req.getEnd().toByteArray());
      }

      private void addConflictKey(AddConflictKeyRequest req) {
        if (logger.isDebugEnabled()) {
          String msg = "AddConflictKeyRequest for: " + printable(req.getKey().toByteArray()) +
              " write: " + req.getWrite();
          logger.debug(msg);
          if (overallSpan != null) {
            overallSpan.event(msg);
          }
        }
        if (req.getWrite()) {
          writeConflictAdds.incrementAndGet();
          tx.addWriteConflictKey(req.getKey().toByteArray());
        } else {
          readConflictAdds.incrementAndGet();
          tx.addReadConflictKey(req.getKey().toByteArray());
        }
      }

      private void addConflictRange(AddConflictRangeRequest req) {
        if (logger.isDebugEnabled()) {
          String msg = "AddConflictRangeRequest from: " + printable(req.getStart().toByteArray()) + " to: " +
              printable(req.getEnd().toByteArray()) + " write: " + req.getWrite();
          logger.debug(msg);
          if (overallSpan != null) {
            overallSpan.event(msg);
          }
        }
        if (req.getWrite()) {
          writeConflictAdds.incrementAndGet();
          tx.addWriteConflictRange(req.getStart().toByteArray(), req.getEnd().toByteArray());
        } else {
          readConflictAdds.incrementAndGet();
          tx.addReadConflictRange(req.getStart().toByteArray(), req.getEnd().toByteArray());
        }
      }

      private void mutateValue(MutateValueRequest req) {
        if (logger.isDebugEnabled()) {
          String msg = "MutateValueRequest for: " + printable(req.getKey().toByteArray()) + " => " +
              printable(req.getParam().toByteArray()) + " with: " + req.getType();
          logger.debug(msg);
          if (overallSpan != null) {
            overallSpan.event(msg);
          }
        }
        rowsMutated.incrementAndGet();
        tx.mutate(getMutationType(req.getType()), req.getKey().toByteArray(), req.getParam().toByteArray());
      }

      /**
       * Use asList() with range gets (only enabled via
       * {@link Configuration.InternalOptions#isUseAsListForRangeGets()}.
       * <p>
       * Normally, calls are routed to
       * {@link #handleRangeGetWithAsyncIterator(StreamingDatabaseRequest, GetRangeRequest, Span, KeySelector, KeySelector, AsyncIterable)}
       */
      private void handleRangeGetWithAsList(GetRangeRequest req, Span opSpan,
                                            KeySelector start, KeySelector end, AsyncIterable<KeyValue> range) {
        Tracer.SpanInScope opScope = tracer.withSpan(opSpan.start());
        range.asList().whenComplete((results, throwable) -> {
          try (Tracer.SpanInScope ignored1 = tracer.withSpan(opSpan)) {
            if (throwable != null) {
              handleThrowable(opSpan, throwable,
                  () -> "failed to get range for start: " + start + " end: " + end +
                      " reverse: " + req.getReverse() + " limit: " + req.getLimit() +
                      " mode: " + req.getStreamingMode());
              sendErrorToRemote(throwable, req.getSequenceId(), responseObserver);
            } else {
              opSpan.tag("rows_read", String.valueOf(results.size()));
              opSpan.tag("batches", String.valueOf(1));
              if (logger.isDebugEnabled()) {
                String msg = "GetRangeRequest from: " + start + " to: " + end + " reverse: " + req.getReverse() +
                    " limit: " + req.getLimit() + " mode: " + req.getStreamingMode() +
                    ", flushing: " + results.size() + " rows, seq_id: " + req.getSequenceId();
                logger.debug(msg);
                opSpan.event(msg);
              }
              if (config.getInternal().isSimulatePartitionsForAsListRangeGets()) {
                if (results.isEmpty()) {
                  synchronized (responseObserver) {
                    responseObserver.onNext(StreamingDatabaseResponse.newBuilder().
                        setGetRange(GetRangeResponse.newBuilder().
                            setDone(true).
                            setSequenceId(req.getSequenceId()).build()).
                        build());
                  }
                }
                List<List<KeyValue>> parts = Lists.partition(results,
                    config.getInternal().getPartitionSizeForAsListRangeGets());
                for (int i = 0; i < parts.size(); i++) {
                  List<KeyValue> keyValues = parts.get(i);
                  boolean done = (i == parts.size() - 1);
                  if (logger.isDebugEnabled()) {
                    String msg = "GetRangeRequest from: " + start + " to: " + end + " reverse: " + req.getReverse() +
                        " limit: " + req.getLimit() + " mode: " + req.getStreamingMode() +
                        ", flushing: " + keyValues.size() + " rows, done: " + done +
                        ", seq_id: " + req.getSequenceId();
                    logger.debug(msg);
                    opSpan.event(msg);
                  }
                  GetRangeResponse.Builder builder = GetRangeResponse.newBuilder().
                      setDone(done).
                      setSequenceId(req.getSequenceId());
                  for (KeyValue result : keyValues) {
                    builder.addKeyValuesBuilder().
                        setKey(ByteString.copyFrom(result.getKey())).
                        setValue(ByteString.copyFrom(result.getValue()));
                  }
                  synchronized (responseObserver) {
                    responseObserver.onNext(StreamingDatabaseResponse.newBuilder().
                        setGetRange(builder.build()).
                        build());
                  }
                }
              } else {
                // do not partition, send as a single batch.
                if (logger.isDebugEnabled()) {
                  String msg = "GetRangeRequest from: " + start + " to: " + end + " reverse: " + req.getReverse() +
                      " limit: " + req.getLimit() + " mode: " + req.getStreamingMode() +
                      ", flushing: " + results.size() + " rows, done: true, seq_id: " + req.getSequenceId();
                  logger.debug(msg);
                  opSpan.event(msg);
                }
                GetRangeResponse.Builder builder = GetRangeResponse.newBuilder().
                    setDone(true).
                    setSequenceId(req.getSequenceId());
                for (KeyValue result : results) {
                  builder.addKeyValuesBuilder().
                      setKey(ByteString.copyFrom(result.getKey())).
                      setValue(ByteString.copyFrom(result.getValue()));
                }
                synchronized (responseObserver) {
                  responseObserver.onNext(StreamingDatabaseResponse.newBuilder().
                      setGetRange(builder.build()).
                      build());
                }
              }
            }
          } finally {
            opScope.close();
            opSpan.end();
          }
        });
      }

      private void handleRangeGetWithAsyncIterator(StreamingDatabaseRequest value, GetRangeRequest req,
                                                   Span opSpan, KeySelector start, KeySelector end,
                                                   AsyncIterable<KeyValue> range) {
        AsyncIterator<KeyValue> iterator = range.iterator();
        // consumer that collects key values and returns them to the user.
        AtomicLong rows = new AtomicLong();
        AtomicLong batches = new AtomicLong();
        BiConsumer<Boolean, Throwable> hasNextConsumer = new BiConsumer<>() {

          private final GetRangeResponse.Builder responseBuilder = GetRangeResponse.newBuilder();

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
                  responseBuilder.addKeyValuesBuilder().
                      setKey(ByteString.copyFrom(next.getKey())).
                      setValue(ByteString.copyFrom(next.getValue()));
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
            int keyValuesCount = responseBuilder.getKeyValuesCount();
            if (!done && keyValuesCount == 0) {
              return;
            }
            batches.incrementAndGet();
            rangeGetBatches.incrementAndGet();
            rowsRead.addAndGet(keyValuesCount);
            if (logger.isDebugEnabled()) {
              String msg = "GetRangeRequest from: " + start + " to: " + end + " reverse: " + req.getReverse() +
                  " limit: " + req.getLimit() + " mode: " + req.getStreamingMode() +
                  ", flushing: " + keyValuesCount + " rows, done: " + done + " seq_id: " + req.getSequenceId();
              logger.debug(msg);
              opSpan.event(msg);
            }
            synchronized (responseObserver) {
              responseObserver.onNext(StreamingDatabaseResponse.newBuilder().
                  setGetRange(responseBuilder.
                      setDone(done).
                      setSequenceId(req.getSequenceId()).
                      build()).
                  build());
            }
            responseBuilder.clear();
          }
        };
        iterator.onHasNext().whenComplete(hasNextConsumer);
      }

      private void addLongLivingFuture(CompletableFuture<?> future) {
        synchronized (this) {
          this.longLivingFutures.add(future);
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
        populateOverallSpanStats();
        longLivingFutures.forEach(x -> x.cancel(false));
        if (tx != null) {
          tx.close();
        }
      }

      @Override
      public void onCompleted() {
        logger.debug("client onCompleted()");
        populateOverallSpanStats();
        longLivingFutures.forEach(x -> x.cancel(false));
        if (tx != null) {
          tx.close();
        }
      }

      private void throwIfSequenceIdHasBeenSeen(long sequenceId) {
        if (!knownSequenceIds.add(sequenceId)) {
          onError(Status.INVALID_ARGUMENT.
              withDescription("sequenceId: " + sequenceId + " has been seen before in this transaction").
              asRuntimeException());
          if (tx != null) {
            tx.close();
          }
        }
      }

      private void populateOverallSpanStats() {
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
          if (keysRead.get() > 0) {
            overallSpan.tag("keys_read", String.valueOf(keysRead.get()));
          }
          if (getApproximateSize.get() > 0) {
            overallSpan.tag("get_approximate_size", String.valueOf(getApproximateSize.get()));
          }
          if (getEstimatedRangeSize.get() > 0) {
            overallSpan.tag("get_estimated_range_size", String.valueOf(getEstimatedRangeSize.get()));
          }
          if (getBoundaryKeys.get() > 0) {
            overallSpan.tag("get_boundary_keys", String.valueOf(getBoundaryKeys.get()));
          }
          if (getAddressesForKey.get() > 0) {
            overallSpan.tag("get_addresses_for_key", String.valueOf(getAddressesForKey.get()));
          }
        }
      }
    };
  }

  private void sendErrorToRemote(Throwable throwable, long sequenceId,
                                 StreamObserver<StreamingDatabaseResponse> responseObserver) {
    OperationFailureResponse.Builder builder = OperationFailureResponse.newBuilder().
        setSequenceId(sequenceId).
        setMessage(throwable.getMessage());
    if (throwable instanceof FDBException) {
      builder.setCode(((FDBException) throwable).getCode());
    }
    //noinspection SynchronizationOnLocalVariableOrMethodParameter
    synchronized (responseObserver) {
      responseObserver.onNext(StreamingDatabaseResponse.newBuilder().
          setOperationFailure(builder.build()).
          build());
    }
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
    Throwable rootCause = Throwables.getRootCause(throwable);
    if (rootCause instanceof FDBException) {
      span.tag("fdb_error_code", String.valueOf(((FDBException) rootCause).getCode())).
          error(rootCause);
      span.tag("fdb_error_message", rootCause.getMessage());
      exceptionMetadata.put(Metadata.Key.of("fdb_error_code", Metadata.ASCII_STRING_MARSHALLER),
          String.valueOf(((FDBException) rootCause).getCode()));
      exceptionMetadata.put(Metadata.Key.of("fdb_error_message", Metadata.ASCII_STRING_MARSHALLER),
          rootCause.getMessage());
    }
    span.event(failureMessage.get());
    logger.warn(failureMessage.get(), rootCause);
    span.error(rootCause);
    return exceptionMetadata;
  }

  /**
   * Handle exceptional cases for a {@link CompletableFuture} and emit an error to the {@link StreamObserver} as well
   * as recording a span.
   */
  private <T> CompletableFuture<T> handleException(CompletableFuture<T> input, Span overallSpan,
                                                   StreamObserver<?> responseObserver,
                                                   String failureMessage) {
    input.whenComplete((t, throwable) -> {
      if (throwable != null) {
        try (Tracer.SpanInScope ignored = tracer.withSpan(overallSpan)) {
          Metadata exceptionMetadata = handleThrowable(overallSpan, throwable, () -> failureMessage);
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
