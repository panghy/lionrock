package io.github.panghy.lionrock.client.foundationdb;

import com.apple.foundationdb.Database;
import io.github.panghy.lionrock.client.foundationdb.impl.RemoteDatabase;
import io.github.panghy.lionrock.proto.TransactionalKeyValueStoreGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Entrypoint for the FoundationDB API-compatible Java client that instead of using native C++ libraries, uses gRPC to
 * communicate with a Lionrock server (which could be the lionrock foundationdb facade). All functionalities of the
 * library is supported except locality related calls (which could be supported in the future via new API methods
 * instead of via {@link com.apple.foundationdb.LocalityUtil}.
 *
 * @author Clement Pang
 */
public class RemoteFoundationDBDatabaseFactory {

  public static final ExecutorService DEFAULT_EXECUTOR;

  static class DaemonThreadFactory implements ThreadFactory {
    private final ThreadFactory factory;
    private static final AtomicInteger threadCount = new AtomicInteger();

    DaemonThreadFactory(ThreadFactory factory) {
      this.factory = factory;
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread t = factory.newThread(r);
      t.setName("fdb-grpc-java-" + threadCount.incrementAndGet());
      t.setDaemon(true);
      return t;
    }
  }

  static {
    ThreadFactory factory = new DaemonThreadFactory(Executors.defaultThreadFactory());
    DEFAULT_EXECUTOR = Executors.newCachedThreadPool(factory);
  }

  /**
   * Open a connection to a remote transactional, key-value store via gRPC.
   *
   * @param channel          The gRPC channel to use.
   * @param databaseName     The database to open (must exist on the remote end).
   * @param clientIdentifier The client identifier.
   * @return A {@link io.github.panghy.lionrock.client.foundationdb.impl.RemoteDatabase}.
   */
  public static Database open(String databaseName,
                              String clientIdentifier,
                              ManagedChannel channel) {
    return new RemoteDatabase(databaseName, clientIdentifier, DEFAULT_EXECUTOR,
        TransactionalKeyValueStoreGrpc.newStub(channel));
  }

  /**
   * Open a connection to a remote transactional, key-value store via gRPC.
   *
   * @param channel          The gRPC channel to use.
   * @param databaseName     The database to open (must exist on the remote end).
   * @param clientIdentifier The client identifier.
   * @param executor         The executor to use.
   * @return A {@link io.github.panghy.lionrock.client.foundationdb.impl.RemoteDatabase}.
   */
  public static Database open(String databaseName,
                              String clientIdentifier,
                              Executor executor,
                              ManagedChannel channel) {
    return new RemoteDatabase(databaseName, clientIdentifier, executor,
        TransactionalKeyValueStoreGrpc.newStub(channel));
  }

  /**
   * Open a connection to a remote transactional, key-value store via gRPC with no encryption. See
   * {@link #open(String, String, Executor, ManagedChannel)} for using a custom built {@link ManagedChannel}.
   *
   * @param host             The database host to connect to.
   * @param port             The port that runs the database service.
   * @param databaseName     The database to open (must exist on the remote end).
   * @param clientIdentifier The client identifier.
   * @return A {@link io.github.panghy.lionrock.client.foundationdb.impl.RemoteDatabase}.
   */
  public static Database openPlainText(String host,
                                       int port,
                                       String databaseName,
                                       String clientIdentifier) {
    return openPlainText(host, port, databaseName, clientIdentifier, DEFAULT_EXECUTOR);
  }

  /**
   * Open a connection to a remote transactional, key-value store via gRPC with no encryption. See
   * {@link #open(String, String, Executor, ManagedChannel)} for using a custom built {@link ManagedChannel}.
   *
   * @param host             The database host to connect to.
   * @param port             The port that runs the database service.
   * @param databaseName     The database to open (must exist on the remote end).
   * @param clientIdentifier The client identifier.
   * @param executor         The executor to use.
   * @return A {@link io.github.panghy.lionrock.client.foundationdb.impl.RemoteDatabase}.
   */
  public static Database openPlainText(String host,
                                       int port,
                                       String databaseName,
                                       String clientIdentifier,
                                       Executor executor) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).
        usePlaintext().
        build();
    return new RemoteDatabase(databaseName, clientIdentifier, executor,
        TransactionalKeyValueStoreGrpc.newStub(channel));
  }
}
