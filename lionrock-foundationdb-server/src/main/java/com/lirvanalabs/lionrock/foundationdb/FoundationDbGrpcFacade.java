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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.apple.foundationdb.tuple.ByteArrayUtil.printable;

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
      logger.debug("Executing DatabaseRequest from: " + request.getClientIdentifier() + " on database: " +
          request.getDatabaseName() + " named: " + request.getName());
    }
    Database db = databaseMap.get(request.getDatabaseName());
    if (db == null) {
      throw Status.INVALID_ARGUMENT.
          withDescription("cannot find database named: " + request.getDatabaseName()).
          asRuntimeException();
    }
    // execute the transaction.
    db.run(tx -> {
      setTransactionDeadline(tx);
      if (request.hasGetValue()) {
        overallSpan.tag("request", "get_value");
        if (logger.isDebugEnabled()) {
          logger.debug("GetValueRequest on: " + printable(request.getGetValue().getKey().toByteArray()));
        }
        tx.get(request.getGetValue().getKey().toByteArray()).whenComplete((val, throwable) -> {
          // create callbackSpan (ensures proper propagation).
          Span callbackSpan = this.tracer.nextSpan(overallSpan).name("execute.get_value.callback");
          try (Tracer.SpanInScope ignored2 = tracer.withSpan(callbackSpan.start())) {
            if (throwable != null) {
              Metadata exceptionMetadata = new Metadata();
              if (throwable instanceof FDBException) {
                exceptionMetadata.put(Metadata.Key.of("fdb_error_code", Metadata.ASCII_STRING_MARSHALLER),
                    String.valueOf(((FDBException) throwable).getCode()));
                exceptionMetadata.put(Metadata.Key.of("fdb_error_message", Metadata.ASCII_STRING_MARSHALLER),
                    throwable.getMessage());
              }
              callbackSpan.error(throwable);
              responseObserver.onError(Status.ABORTED.
                  withCause(throwable).
                  withDescription(throwable.getMessage()).
                  asRuntimeException(exceptionMetadata));
            } else {
              GetValueResponse.Builder build = GetValueResponse.newBuilder();
              if (logger.isDebugEnabled()) {
                logger.debug("GetValueRequest on: " +
                    printable(request.getGetValue().getKey().toByteArray()) + " is: " +
                    printable(val));
              }
              if (val != null) {
                build.setValue(ByteString.copyFrom(val));
              }
              responseObserver.onNext(DatabaseResponse.newBuilder().
                  setGetValue(build.build()).
                  build());
              responseObserver.onCompleted();
            }
          } finally {
            callbackSpan.end();
          }
        });
      }
      return null;
    });
  }

  @Override
  public StreamObserver<StreamingDatabaseRequest> executeTransaction(
      StreamObserver<StreamingDatabaseResponse> responseObserver) {
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
          logger.debug("Starting transaction " + startRequest.getName() + " against db: " +
              startRequest.getDatabaseName());
          Database db = databaseMap.get(startRequest.getDatabaseName());
          if (db == null) {
            StatusRuntimeException toThrow = Status.INVALID_ARGUMENT.
                withDescription("cannot find database named: " + startRequest.getDatabaseName()).
                asRuntimeException();
            responseObserver.onError(toThrow);
            throw toThrow;
          }
          tx = db.createTransaction();
          if (overallSpan != null) {
            overallSpan.tag("client", startRequest.getClientIdentifier()).
                tag("database_name", startRequest.getDatabaseName()).
                tag("name", startRequest.getName());
          }
        } else if (value.hasCommitTransaction()) {
          logger.debug("CommitTransactionRequest");
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
          tx.commit().handle((unused, throwable) -> {
            Span callbackSpan = tracer.nextSpan(opSpan).name("execute_transaction.commit_transaction.callback");
            try (Tracer.SpanInScope ignored = tracer.withSpan(callbackSpan.start())) {
              if (throwable != null) {
                Metadata exceptionMetadata = handleThrowable(opSpan, throwable);
                StatusRuntimeException toThrow =
                    Status.ABORTED.withCause(throwable).
                        withDescription("failed to commit transaction").
                        asRuntimeException(exceptionMetadata);
                responseObserver.onError(toThrow);
                logger.warn("failed to commit transaction", toThrow);
              } else {
                responseObserver.onNext(StreamingDatabaseResponse.newBuilder().
                    setCommitTransaction(CommitTransactionResponse.newBuilder().
                        setCommittedVersion(tx.getCommittedVersion()).build()).build());
                if (logger.isDebugEnabled()) {
                  logger.debug("Committed transaction: " + tx.getCommittedVersion());
                }
              }
            } catch (Throwable t) {
              handleThrowable(callbackSpan, t);
            } finally {
              opScope.close();
              opSpan.end();
            }
            return null;
          });
        } else if (value.hasGetValue()) {
          if (logger.isDebugEnabled()) {
            logger.debug("GetValueRequest on: " + printable(value.getGetValue().getKey().toByteArray()));
          }
          hasActiveTransactionOrThrow();
          // start the span/scope for the get_value call.
          Span opSpan = tracer.nextSpan(overallSpan).name("execute_transaction.get_value");
          Tracer.SpanInScope opScope = tracer.withSpan(opSpan.start());
          GetValueRequest getValueRequest = value.getGetValue();
          tx.get(getValueRequest.getKey().toByteArray()).whenComplete((val, throwable) -> {
            Span callbackSpan = tracer.nextSpan(opSpan).name("execute_transaction.get_value.callback");
            try (Tracer.SpanInScope ignored = tracer.withSpan(callbackSpan.start())) {
              if (throwable != null) {
                handleThrowable(opSpan, throwable);
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
                  logger.debug("GetValueRequest on: " +
                      printable(value.getGetValue().getKey().toByteArray()) + " is: " +
                      printable(val));
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
            } catch (Throwable t) {
              handleThrowable(callbackSpan, t);
            } finally {
              opScope.close();
              opSpan.end();
              callbackSpan.end();
            }
          });
        } else if (value.hasSetValue()) {
          hasActiveTransactionOrThrow();
          if (logger.isDebugEnabled()) {
            logger.debug("SetValueRequest for: " +
                printable(value.getSetValue().getKey().toByteArray()) + " => " +
                printable(value.getSetValue().getValue().toByteArray()));
          }
          tx.set(value.getSetValue().getKey().toByteArray(),
              value.getSetValue().getValue().toByteArray());
        } else if (value.hasClearKey()) {
          hasActiveTransactionOrThrow();
          if (logger.isDebugEnabled()) {
            logger.debug("ClearKeyRequest for: " +
                printable(value.getClearKey().getKey().toByteArray()));
          }
          tx.clear(value.getClearKey().getKey().toByteArray());
        }
      }

      private Metadata handleThrowable(Span span, Throwable throwable) {
        Metadata exceptionMetadata = new Metadata();
        if (throwable instanceof FDBException) {
          span.tag("fdb_error_code", String.valueOf(((FDBException) throwable).getCode())).
              error(throwable);
          exceptionMetadata.put(Metadata.Key.of("fdb_error_code", Metadata.ASCII_STRING_MARSHALLER),
              String.valueOf(((FDBException) throwable).getCode()));
          exceptionMetadata.put(Metadata.Key.of("fdb_error_message", Metadata.ASCII_STRING_MARSHALLER),
              throwable.getMessage());
        }
        span.error(throwable);
        return exceptionMetadata;
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

  private void setTransactionDeadline(Transaction tx) {
    if (Context.current().getDeadline() != null) {
      long value = Context.current().getDeadline().timeRemaining(TimeUnit.MILLISECONDS);
      if (value <= 0) {
        throw Status.DEADLINE_EXCEEDED.asRuntimeException();
      }
      tx.options().setTimeout(value);
    } else {
      tx.options().setTimeout(config.getDefaultFdbTimeoutMs());
    }
  }

  public static void main(String[] args) {
    SpringApplication.run(FoundationDbGrpcFacade.class, args);
  }
}
