package io.github.panghy.lionrock.foundationdb.record;

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import io.github.panghy.lionrock.foundationdb.AbstractGrpcTest;
import org.junit.jupiter.api.Test;

public class RemoteRecordLayerTests extends AbstractGrpcTest {
  @Test
  public void testConnectivity() {
    RemoteFDBDatabaseFactory factory = new RemoteFDBDatabaseFactory(channel, "RemoteRecordLayerTests");
    FDBDatabase database = factory.getDatabase();
  }
}
