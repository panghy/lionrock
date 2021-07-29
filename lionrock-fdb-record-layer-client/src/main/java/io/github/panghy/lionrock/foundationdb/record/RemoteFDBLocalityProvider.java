package io.github.panghy.lionrock.foundationdb.record;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.CloseableAsyncIterator;
import com.apple.foundationdb.record.provider.foundationdb.FDBLocalityProvider;
import io.github.panghy.lionrock.client.foundationdb.RemoteLocalityUtil;

import javax.annotation.Nonnull;

/**
 * Adapter for {@link RemoteLocalityUtil}.
 *
 * @author Clement Pang
 */
public class RemoteFDBLocalityProvider implements FDBLocalityProvider {

  private static final RemoteFDBLocalityProvider INSTANCE = new RemoteFDBLocalityProvider();

  private RemoteFDBLocalityProvider() {
  }

  @Nonnull
  public static RemoteFDBLocalityProvider instance() {
    return INSTANCE;
  }

  @Nonnull
  @Override
  public CloseableAsyncIterator<byte[]> getBoundaryKeys(
      @Nonnull Transaction transaction, @Nonnull byte[] start, @Nonnull byte[] end) {
    return RemoteLocalityUtil.getBoundaryKeys(transaction, start, end);
  }
}
