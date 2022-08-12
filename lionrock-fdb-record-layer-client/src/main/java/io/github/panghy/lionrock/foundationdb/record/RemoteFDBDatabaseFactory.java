package io.github.panghy.lionrock.foundationdb.record;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBLocalityProvider;
import com.apple.foundationdb.record.provider.foundationdb.FDBTraceFormat;
import io.github.panghy.lionrock.client.foundationdb.RemoteFoundationDBDatabaseFactory;
import io.github.panghy.lionrock.proto.TransactionalKeyValueStoreGrpc;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Supplier;

/**
 * Implementation of {@link FDBDatabaseFactory} that's backed by {@link RemoteFoundationDBDatabaseFactory} and talks to
 * an FDB instance not via native libraries via gRPC.
 *
 * @author Clement Pang
 */
public class RemoteFDBDatabaseFactory extends FDBDatabaseFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteFDBDatabaseFactory.class);

  private final TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub;
  private final String clientIdentifier;
  /**
   * The default is a log-based predicate, which can also be used to enable tracing on a more granular level
   * (such as by request) using {@link #setTransactionIsTracedSupplier(Supplier)}.
   */
  @Nonnull
  private Supplier<Boolean> transactionIsTracedSupplier = LOGGER::isTraceEnabled;

  @Nonnull
  private FDBLocalityProvider localityProvider = RemoteFDBLocalityProvider.instance();

  /**
   * Create a new {@link RemoteFDBDatabaseFactory}.
   *
   * @param channel          The gRPC {@link ManagedChannel} to talk to the backend.
   * @param clientIdentifier The client identifier when communicating with the server.
   */
  public RemoteFDBDatabaseFactory(ManagedChannel channel, String clientIdentifier) {
    this.stub = TransactionalKeyValueStoreGrpc.newStub(channel);
    this.clientIdentifier = clientIdentifier;
  }

  /**
   * Create a new {@link RemoteFDBDatabaseFactory}.
   *
   * @param stub             The {@link io.github.panghy.lionrock.proto.TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub} to use.
   * @param clientIdentifier The client identifier when communicating with the server.
   */
  public RemoteFDBDatabaseFactory(TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub,
                                  String clientIdentifier) {
    this.stub = stub;
    this.clientIdentifier = clientIdentifier;
  }

  @Override
  public void shutdown() {
  }

  @Override
  public void setTrace(@Nullable String s, @Nullable String s1) {
  }

  @Override
  public void setTraceFormat(@Nonnull FDBTraceFormat fdbTraceFormat) {
  }

  @Override
  public void setRunLoopProfilingEnabled(boolean b) {
  }

  @Override
  public boolean isRunLoopProfilingEnabled() {
    return false;
  }

  @Override
  public void setTransactionIsTracedSupplier(Supplier<Boolean> transactionIsTracedSupplier) {
    this.transactionIsTracedSupplier = transactionIsTracedSupplier;
  }

  @Override
  public Supplier<Boolean> getTransactionIsTracedSupplier() {
    return transactionIsTracedSupplier;
  }

  @Override
  @Nonnull
  public synchronized FDBDatabase getDatabase(@Nullable String clusterFile) {
    FDBDatabase database = databases.get(clusterFile);
    if (database == null) {
      database = new FDBDatabase(this, clusterFile);
      database.setDirectoryCacheSize(getDirectoryCacheSize());
      database.setTrackLastSeenVersion(getTrackLastSeenVersion());
      database.setResolverStateRefreshTimeMillis(getStateRefreshTimeMillis());
      database.setDatacenterId(getDatacenterId());
      database.setStoreStateCache(storeStateCacheFactory.getCache(database));
      databases.put(clusterFile, database);
    }
    return database;
  }

  @Override
  @Nonnull
  public synchronized FDBDatabase getDatabase() {
    return getDatabase(null);
  }

  @Override
  @Nonnull
  public FDBLocalityProvider getLocalityProvider() {
    return localityProvider;
  }

  @Override
  public void setLocalityProvider(@Nonnull FDBLocalityProvider localityProvider) {
    this.localityProvider = localityProvider;
  }

  @Nonnull
  @Override
  public Database open(String databaseName) {
    if (databaseName == null) {
      databaseName = "fdb";
    }
    return RemoteFoundationDBDatabaseFactory.open(databaseName, clientIdentifier, stub);
  }
}
