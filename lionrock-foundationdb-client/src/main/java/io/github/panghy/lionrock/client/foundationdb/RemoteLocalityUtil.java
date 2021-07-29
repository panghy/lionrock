package io.github.panghy.lionrock.client.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.CloseableAsyncIterator;
import io.github.panghy.lionrock.client.foundationdb.impl.RemoteDatabase;
import io.github.panghy.lionrock.client.foundationdb.impl.RemoteTransaction;

import java.util.concurrent.CompletableFuture;

/**
 * Similar to {@code com.apple.foundationdb.LocalityUtil}, provides locality information from a remote lionrock server
 * (if supported).
 *
 * @author Clement Pang
 */
public class RemoteLocalityUtil {

  public static CloseableAsyncIterator<byte[]> getBoundaryKeys(Database db, byte[] begin, byte[] end) {
    if (!(db instanceof RemoteDatabase)) {
      throw new IllegalArgumentException("only RemoteDatabase is supported, got: " + db);
    }
    RemoteTransaction transaction = (RemoteTransaction) db.createTransaction();
    AsyncIterator<byte[]> boundaryKeys = transaction.getBoundaryKeys(begin, end).iterator();
    return new CloseableAsyncIterator<>() {
      @Override
      public void close() {
        boundaryKeys.cancel();
        transaction.close();
      }

      @Override
      public CompletableFuture<Boolean> onHasNext() {
        return boundaryKeys.onHasNext();
      }

      @Override
      public boolean hasNext() {
        return boundaryKeys.hasNext();
      }

      @Override
      public byte[] next() {
        return boundaryKeys.next();
      }
    };
  }

  public static CloseableAsyncIterator<byte[]> getBoundaryKeys(Transaction tr, byte[] begin, byte[] end) {
    if (!(tr instanceof RemoteTransaction)) {
      throw new IllegalArgumentException("must be RemoteTransaction, got: " + tr);
    }
    RemoteTransaction transaction = (RemoteTransaction) tr;
    AsyncIterator<byte[]> boundaryKeys = transaction.getBoundaryKeys(begin, end).iterator();
    return new CloseableAsyncIterator<>() {
      @Override
      public void close() {
        boundaryKeys.cancel();
      }

      @Override
      public CompletableFuture<Boolean> onHasNext() {
        return boundaryKeys.onHasNext();
      }

      @Override
      public boolean hasNext() {
        return boundaryKeys.hasNext();
      }

      @Override
      public byte[] next() {
        return boundaryKeys.next();
      }
    };
  }

  public static CompletableFuture<String[]> getAddressesForKey(Transaction tr, byte[] key) {
    if (!(tr instanceof RemoteTransaction)) {
      throw new IllegalArgumentException("must be RemoteTransaction, got: " + tr);
    }
    RemoteTransaction transaction = (RemoteTransaction) tr;
    return ((RemoteTransaction) tr).getAddressesForKey(key);
  }
}
