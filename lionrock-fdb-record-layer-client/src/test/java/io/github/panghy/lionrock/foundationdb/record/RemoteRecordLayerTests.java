package io.github.panghy.lionrock.foundationdb.record;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.DirectoryLayerDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.sample.SampleProto;
import io.github.panghy.lionrock.foundationdb.AbstractGrpcTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RemoteRecordLayerTests extends AbstractGrpcTest {

  private FDBRecordStore.Builder recordStoreBuilder;
  private FDBDatabase fdb;

  @Test
  public void testReadingVendorRecordWithPrimaryKey() {
    SampleProto.Vendor.Builder readBuilder = fdb.run((FDBRecordContext cx) -> {
      FDBRecordStore store = recordStoreBuilder.copyBuilder().setContext(cx).open();
      return SampleProto.Vendor.newBuilder()
          .mergeFrom(store.loadRecord(Key.Evaluated.scalar(9375L).toTuple()).getRecord());
    });
    assertEquals(9375L, readBuilder.getVendorId());
    assertEquals("Acme", readBuilder.getVendorName());
  }

  @BeforeEach
  public void setupConnection() {
    RemoteFDBDatabaseFactory factory = new RemoteFDBDatabaseFactory(channel, "RemoteRecordLayerTests");
    this.fdb = factory.getDatabase();

    final KeySpace keySpace = new KeySpace(
        new DirectoryLayerDirectory("application")
            .addSubdirectory(new KeySpaceDirectory("environment", KeySpaceDirectory.KeyType.STRING)));
    final KeySpacePath path = keySpace.path("application", "record-layer-sample")
        .add("environment", "demo");
    fdb.runAsync(path::deleteAllDataAsync);

    RecordMetaData rmd = RecordMetaData.build(SampleProto.getDescriptor());
    this.recordStoreBuilder = FDBRecordStore.newBuilder()
        .setMetaDataProvider(rmd)
        .setKeySpacePath(path);
    writeVendorAndItemRecord();
  }

  public void writeVendorAndItemRecord() {
    fdb.run((FDBRecordContext cx) -> {
      FDBRecordStore store = recordStoreBuilder.copyBuilder().setContext(cx).create();
      store.saveRecord(SampleProto.Vendor.newBuilder()
          .setVendorId(9375L)
          .setVendorName("Acme")
          .build());
      store.saveRecord(SampleProto.Vendor.newBuilder()
          .setVendorId(1066L)
          .setVendorName("Buy n Large")
          .build());
      store.saveRecord(SampleProto.Item.newBuilder()
          .setItemId(4836L)
          .setItemName("GPS")
          .setVendorId(9375L)
          .build());
      store.saveRecord(SampleProto.Item.newBuilder()
          .setItemId(9970L)
          .setItemName("Personal Transport")
          .setVendorId(1066L)
          .build());
      store.saveRecord(SampleProto.Item.newBuilder()
          .setItemId(8380L)
          .setItemName("Piles of Garbage")
          .setVendorId(1066L)
          .build());
      return null;
    });
  }
}
