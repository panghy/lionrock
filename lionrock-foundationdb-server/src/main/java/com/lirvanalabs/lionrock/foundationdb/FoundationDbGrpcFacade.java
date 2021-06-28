package com.lirvanalabs.lionrock.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.google.protobuf.ByteString;
import com.lirvanalabs.lionrock.proto.*;
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.apple.foundationdb.tuple.ByteArrayUtil.printable;
import static java.util.concurrent.CompletableFuture.completedFuture;

@SpringBootApplication
@GRpcService
@Component
public class FoundationDbGrpcFacade extends TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreImplBase {

  static final CompletableFuture<?> DONE = completedFuture(null);

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
        setTransactionDeadline(rpcContext, tx);
        return tx.get(request.getGetValue().getKey().toByteArray());
      }), overallSpan, responseObserver, "failed to get key").thenAccept(val -> {
        try (Tracer.SpanInScope ignored2 = tracer.withSpan(overallSpan)) {
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
                setTransactionDeadline(rpcContext, tx);
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
                setTransactionDeadline(rpcContext, tx);
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
                setTransactionDeadline(rpcContext, tx);
                tx.clear(request.getClearRange().getStart().toByteArray(),
                    request.getClearRange().getEnd().toByteArray());
                return completedFuture(tx);
              }), overallSpan, responseObserver, "failed to set key"),
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
      private volatile Transaction tx;

      @Override
      public void onNext(StreamingDatabaseRequest value) {
        if (value.hasStartTransaction()) {
          StartTransactionRequest startRequest = this.startRequest.updateAndGet(startTransactionRequest -> {
            if (startTransactionRequest != null) {
              StatusRuntimeException toThrow = Status.INVALID_ARGUMENT.
                      withDescription("cannot send StartTransactionRequest twice").
                      asRuntimeException();
              responseObserver.onError(toThrow);
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
            responseObserver.onError(toThrow);
            throw toThrow;
          }
          tx = db.createTransaction();
          setTransactionDeadline(rpcContext, tx);
          if (overallSpan != null) {
            overallSpan.tag("client", startRequest.getClientIdentifier()).
                tag("database_name", startRequest.getDatabaseName()).
                tag("name", startRequest.getName());
          }
        } else if (value.hasCommitTransaction()) {
          if (logger.isDebugEnabled()) {
            overallSpan.event("CommitTransactionRequest");
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
                  responseObserver.onNext(StreamingDatabaseResponse.newBuilder().
                          setCommitTransaction(CommitTransactionResponse.newBuilder().
                                  setCommittedVersion(tx.getCommittedVersion()).build()).build());
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
          tx.get(getValueRequest.getKey().toByteArray()).whenComplete((val, throwable) -> {
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
                responseObserver.onNext(StreamingDatabaseResponse.newBuilder().
                    setOperationFailure(builder.build()).
                    build());
              } else {
                if (logger.isDebugEnabled()) {
                  String msg = "GetValueRequest on: " +
                          printable(value.getGetValue().getKey().toByteArray()) + " is: " +
                          printable(val);
                  logger.debug(msg);
                  opSpan.event(msg);
                }
                GetValueResponse.Builder build = GetValueResponse.newBuilder().
                    setSequenceId(value.getGetValue().getSequenceId());
                if (val != null) {
                  build.setValue(ByteString.copyFrom(val));
                }
                responseObserver.onNext(StreamingDatabaseResponse.newBuilder().
                    setGetValue(build.build()).
                    build());
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
          tx.clear(value.getClearRange().getStart().toByteArray(),
              value.getClearRange().getEnd().toByteArray());
        }
      }

      private void hasActiveTransactionOrThrow() {
        if (tx == null) {
          StatusRuntimeException toThrow = Status.INVALID_ARGUMENT.
              withDescription("must have an active transaction").
              asRuntimeException();
          responseObserver.onError(toThrow);
          throw toThrow;
        }
      }

      @Override
      public void onError(Throwable t) {
        logger.warn("onError from client in executeTransaction");
        if (tx != null) {
          tx.cancel();
          tx.close();
        }
      }

      @Override
      public void onCompleted() {
        // close the transaction if active (in-flight futures will be cancelled if the client calls this).
        // TODO: for watches, we might need to keep the channel open forever? perhaps then the client would just not
        //  close the connection?
        if (tx != null) {
          if (!commitStarted.get()) {
            tx.cancel();
          }
          tx.close();
        }
        // when the client closes the connection, we also close the connection to it.
        try {
          responseObserver.onCompleted();
        } catch (IllegalStateException ex) {
          // ignored.
        }
      }
    };
  }

  private void setTransactionDeadline(Context rpcContext, Transaction tx) {
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
      responseObserver.onNext(DatabaseResponse.newBuilder().
          setCommittedTransaction(CommitTransactionResponse.newBuilder().build()).
          build());
      responseObserver.onCompleted();
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
          responseObserver.onError(Status.ABORTED.
              withCause(throwable).
              withDescription(throwable.getMessage()).
              asRuntimeException(exceptionMetadata));
        }
      }
    });
    return input;
  }

  public static void main(String[] args) {
    SpringApplication.run(FoundationDbGrpcFacade.class, args);
  }
}
