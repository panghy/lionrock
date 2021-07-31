package io.github.panghy.lionrock.foundationdb.record;

import com.apple.foundationdb.FDBError;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.*;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.*;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.DirectoryLayerDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.sample.SampleProto;
import com.google.common.base.Joiner;
import com.google.protobuf.Message;
import io.github.panghy.lionrock.foundationdb.AbstractGrpcTest;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RemoteRecordLayerTests extends AbstractGrpcTest {

  private static final Logger logger = LoggerFactory.getLogger(RemoteRecordLayerTests.class);

  private FDBRecordStore.Builder recordStoreBuilder;
  private FDBDatabase fdb;
  private KeySpacePath path;

  public static List<String> readNames(FDBRecordStore.Builder recordStoreBuilder, FDBRecordContext cx, RecordQuery query) {
    List<String> names = new ArrayList<>();
    FDBRecordStore store = recordStoreBuilder.copyBuilder().setContext(cx).open();
    RecordQueryPlan plan = store.planQuery(query);
    try (RecordCursor<FDBQueriedRecord<Message>> cursor = store.executeQuery(plan)) {
      RecordCursorResult<FDBQueriedRecord<Message>> result;
      do {
        result = cursor.getNext();
        if (result.hasNext()) {
          SampleProto.Customer.Builder builder = SampleProto.Customer.newBuilder().mergeFrom(result.get().getRecord());
          names.add(builder.getFirstName() + " " + builder.getLastName());
        }
      } while (result.hasNext());
    }
    return names;
  }

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

  @Test
  public void testLookForItemIdsWithVendorId() {
    ArrayList<Long> ids = fdb.run((FDBRecordContext cx) -> {
      ArrayList<Long> itemIDs = new ArrayList<>();
      FDBRecordStore store = recordStoreBuilder.copyBuilder().setContext(cx).open();
      RecordQuery query = RecordQuery.newBuilder()
          .setRecordType("Item")
          .setFilter(Query.field("vendor_id").equalsValue(9375L))
          .build();
      try (RecordCursor<FDBQueriedRecord<Message>> cursor = store.executeQuery(query)) {
        RecordCursorResult<FDBQueriedRecord<Message>> result;
        do {
          result = cursor.getNext();
          if (result.hasNext()) {
            itemIDs.add(SampleProto.Item.newBuilder()
                .mergeFrom(result.get().getRecord())
                .getItemId());
          }
        } while (result.hasNext());
      }

      return itemIDs;
    });
    assertTrue(ids.contains(4836L));
    ids = fdb.run((FDBRecordContext cx) -> {
      ArrayList<Long> itemIDs = new ArrayList<>();
      FDBRecordStore store = recordStoreBuilder.copyBuilder().setContext(cx).open();
      RecordQuery query = RecordQuery.newBuilder()
          .setRecordType("Item")
          .setFilter(Query.field("vendor_id").equalsValue(1066L))
          .build();
      try (RecordCursor<FDBQueriedRecord<Message>> cursor = store.executeQuery(query)) {
        RecordCursorResult<FDBQueriedRecord<Message>> result;
        do {
          result = cursor.getNext();
          if (result.hasNext()) {
            itemIDs.add(SampleProto.Item.newBuilder()
                .mergeFrom(result.get().getRecord())
                .getItemId());
          }
        } while (result.hasNext());
      }

      return itemIDs;
    });
    assertTrue(ids.contains(9970L));
    assertTrue(ids.contains(8380L));
  }

  @Test
  public void testGroupingItemsByVendor() {
    Map<String, List<String>> namesToItems = fdb.runAsync((FDBRecordContext cx) -> recordStoreBuilder.copyBuilder().setContext(cx).openAsync().thenCompose(store -> {
      // Outer plan gets all of the vendors
      RecordQueryPlan outerPlan = store.planQuery(RecordQuery.newBuilder()
          .setRecordType("Vendor")
          .setRequiredResults(Arrays.asList(field("vendor_id"), field("vendor_name")))
          .build());
      // Inner plan gets all items for the given vendor ID.
      // Using "equalsParameter" does the plan once and re-uses the plan for each vendor ID.
      RecordQueryPlan innerPlan = store.planQuery(RecordQuery.newBuilder()
          .setRecordType("Item")
          .setRequiredResults(Collections.singletonList(field("item_name")))
          .setFilter(Query.field("vendor_id").equalsParameter("vid"))
          .build());
      return store.executeQuery(outerPlan)
          // Step 1: Get all of the vendors and initiate a query for items with their vendor ID.
          .mapPipelined(record -> {
            SampleProto.Vendor vendor = SampleProto.Vendor.newBuilder().mergeFrom(record.getRecord()).build();
            return innerPlan.execute(store, EvaluationContext.forBinding("vid", vendor.getVendorId()))
                .map(innerRecord -> SampleProto.Item.newBuilder().mergeFrom(innerRecord.getRecord()).getItemName())
                .asList()
                .thenApply(list -> Pair.of(vendor.getVendorName(), list));
          }, 10)
          .asList()

          // Step 2: Collect the results of the subqueries and package them as a map.
          .thenApply((List<Pair<String, List<String>>> list) ->
              list.stream().collect(Collectors.toMap(Pair::getKey, Pair::getValue))
          );
    })).join();
    assertTrue(namesToItems.containsKey("Acme"));
    assertTrue(namesToItems.containsKey("Buy n Large"));
    assertTrue(namesToItems.get("Acme").contains("GPS"));
    assertTrue(namesToItems.get("Buy n Large").contains("Personal Transport"));
    assertTrue(namesToItems.get("Buy n Large").contains("Piles of Garbage"));
  }

  @Test
  public void testRichIndex_recordCount() {
    Long recordCount = fdb.runAsync((FDBRecordContext cx) ->
        recordStoreBuilder.copyBuilder().setContext(cx).openAsync()
            .thenCompose(FDBRecordStore::getSnapshotRecordCount)
    ).join();
    assertEquals(7, recordCount);
  }

  @Test
  public void testRichIndex_allCustomersWithFirstName() {
    List<String> names = fdb.run((FDBRecordContext cx) -> {
      RecordQuery query = RecordQuery.newBuilder()
          .setRecordType("Customer")
          .setFilter(Query.field("first_name").equalsValue("Jane"))
          .build();
      return readNames(recordStoreBuilder, cx, query);
    });
    assertEquals(1, names.size());
    assertTrue(names.contains("Jane Doe"));
  }

  @Test
  public void testRichIndex_allCustomersWithLastName() {
    List<String> names = fdb.run((FDBRecordContext cx) -> {
      RecordQuery query = RecordQuery.newBuilder()
          .setRecordType("Customer")
          .setFilter(Query.field("last_name").equalsValue("Doe"))
          .build();
      return readNames(recordStoreBuilder, cx, query);
    });
    assertEquals(1, names.size());
    assertTrue(names.contains("Jane Doe"));
  }

  @Test
  public void testRichIndex_allCustomersWithName() {
    List<String> names = fdb.run((FDBRecordContext cx) -> {
      RecordQuery query = RecordQuery.newBuilder()
          .setRecordType("Customer")
          .setFilter(Query.and(
              Query.field("first_name").equalsValue("Jane"),
              Query.field("last_name").equalsValue("Doe"))
          )
          .build();
      return readNames(recordStoreBuilder, cx, query);
    });
    assertEquals(1, names.size());
    assertTrue(names.contains("Jane Doe"));
  }

  @Test
  public void testRichIndex_allCustomersWithPreferenceTagsBooksAndMovies() {
    List<String> names = fdb.run((FDBRecordContext cx) -> {
      RecordQuery query = RecordQuery.newBuilder()
          .setRecordType("Customer")
          .setFilter(Query.and(
              Query.field("preference_tag").oneOfThem().equalsValue("books"),
              Query.field("preference_tag").oneOfThem().equalsValue("movies"))
          )
          .build();
      return readNames(recordStoreBuilder, cx, query);
    });
    assertEquals(1, names.size());
    assertTrue(names.contains("John Smith"));
  }

  @Test
  public void testRichIndex_getNumberOfCustomersWhoHaveBooksListedAsPreferenceTags() {
    Long bookPreferenceCount = fdb.runAsync((FDBRecordContext cx) -> recordStoreBuilder.copyBuilder().setContext(cx).openAsync().thenCompose(store -> {
      Index index = store.getRecordMetaData().getIndex("preference_tag_count");
      IndexAggregateFunction function = new IndexAggregateFunction(FunctionNames.COUNT, index.getRootExpression(), index.getName());
      return store.evaluateAggregateFunction(Collections.singletonList("Customer"), function, Key.Evaluated.scalar("books"), IsolationLevel.SERIALIZABLE)
          .thenApply(tuple -> tuple.getLong(0));
    })).join();
    assertEquals(2, bookPreferenceCount);
  }

  @Test
  public void testRichIndex_allCustomersWithAnOrderOfQuantityGreaterThan2() {
    List<String> names = fdb.run((FDBRecordContext cx) -> {
      RecordQuery query = RecordQuery.newBuilder()
          .setRecordType("Customer")
          .setFilter(Query.field("order").oneOfThem().matches(Query.field("quantity").greaterThan(2)))
          .build();
      return readNames(recordStoreBuilder, cx, query);
    });
    assertEquals(1, names.size());
    assertTrue(names.contains("Jane Doe"));
  }

  @Test
  public void testSumOfQuantityOfItemsForItem2740() {
    Long itemQuantitySum = fdb.runAsync((FDBRecordContext cx) -> recordStoreBuilder.copyBuilder().setContext(cx).openAsync().thenCompose(store -> {
      Index index = store.getRecordMetaData().getIndex("item_quantity_sum");
      IndexAggregateFunction function = new IndexAggregateFunction(FunctionNames.SUM, index.getRootExpression(), index.getName());
      return store.evaluateAggregateFunction(Collections.singletonList("Customer"), function, Key.Evaluated.scalar(2740L), IsolationLevel.SERIALIZABLE)
          .thenApply(tuple -> tuple.getLong(0));
    })).join();
    assertEquals(4, itemQuantitySum);
  }

  @Test
  public void testSumOfTheQuantityOfAllItemsOrdered() {
    Long allItemsQuantitySum = fdb.runAsync((FDBRecordContext cx) -> recordStoreBuilder.copyBuilder().setContext(cx).openAsync().thenCompose(store -> {
      Index index = store.getRecordMetaData().getIndex("item_quantity_sum");
      IndexAggregateFunction function = new IndexAggregateFunction(FunctionNames.SUM, index.getRootExpression(), index.getName());
      return store.evaluateAggregateFunction(Collections.singletonList("Customer"), function, TupleRange.ALL, IsolationLevel.SERIALIZABLE)
          .thenApply(tuple -> tuple.getLong(0));
    })).join();
    assertEquals(6, allItemsQuantitySum);
  }

  @BeforeEach
  public void setupConnection() {
    logger.info("setupConnection()");
    RemoteFDBDatabaseFactory factory = new RemoteFDBDatabaseFactory(channel, "RemoteRecordLayerTests");
    this.fdb = factory.getDatabase();
    this.fdb.setAsyncToSyncExceptionMapper((ex, event) -> {
      if (ex instanceof ExecutionException || ex instanceof CompletionException) {
        ex = ex.getCause();
      }
      if (ex instanceof FDBException) {
        FDBException fdbex = (FDBException) ex;
        switch (FDBError.fromCode(fdbex.getCode())) {
          case TRANSACTION_TOO_OLD:
            return (new FDBExceptions.FDBStoreTransactionIsTooOldException(fdbex));
          case NOT_COMMITTED:
            return (new FDBExceptions.FDBStoreTransactionConflictException(fdbex));
          case TRANSACTION_TIMED_OUT:
            return (new FDBExceptions.FDBStoreTransactionTimeoutException(fdbex));
          case TRANSACTION_TOO_LARGE:
            return (new FDBExceptions.FDBStoreTransactionSizeException(fdbex));
          case KEY_TOO_LARGE:
            return (new FDBExceptions.FDBStoreKeySizeException(fdbex));
          case VALUE_TOO_LARGE:
            return (new FDBExceptions.FDBStoreValueSizeException(fdbex));
          default:
            return fdbex.isRetryable() ? (new FDBExceptions.FDBStoreRetriableException(fdbex)) :
                (new FDBExceptions.FDBStoreException(fdbex));
        }
      } else if (ex instanceof RuntimeException) {
        return (RuntimeException) ex;
      } else {
        return ex instanceof InterruptedException ? (new RecordCoreInterruptedException(ex.getMessage(), new Object[]{ex})) : (new RecordCoreException(ex.getMessage(), ex));
      }
    });

    final KeySpace keySpace = new KeySpace(
        new DirectoryLayerDirectory("application")
            .addSubdirectory(new KeySpaceDirectory("environment", KeySpaceDirectory.KeyType.STRING)));
    this.path = keySpace.path("application", "record-layer-sample").add("environment", "demo");
    this.fdb.runAsync(path::deleteAllDataAsync);

    RecordMetaDataBuilder rmdBuilder = RecordMetaData.newBuilder().setRecords(SampleProto.getDescriptor());
    rmdBuilder.getRecordType("Customer").setPrimaryKey(
        concatenateFields("last_name", "first_name", "customer_id"));
    rmdBuilder.addUniversalIndex(new Index("globalCount",
        new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));
    rmdBuilder.addIndex("Customer", new Index("first_name",
        field("first_name"), IndexTypes.VALUE));
    rmdBuilder.addIndex("Customer", new Index("email_address",
        field("email_address", KeyExpression.FanType.FanOut),
        IndexTypes.VALUE));
    rmdBuilder.addIndex("Customer", new Index("preference_tag",
        field("preference_tag", KeyExpression.FanType.Concatenate),
        IndexTypes.VALUE));
    rmdBuilder.addIndex("Customer", new Index("preference_tag_count",
        new GroupingKeyExpression(field("preference_tag", KeyExpression.FanType.FanOut), 0),
        IndexTypes.COUNT));
    rmdBuilder.addIndex("Customer", new Index("order",
        field("order", KeyExpression.FanType.FanOut).nest("quantity"),
        IndexTypes.VALUE));
    rmdBuilder.addIndex("Customer", new Index("item_quantity_sum",
        new GroupingKeyExpression(field("order", KeyExpression.FanType.FanOut)
            .nest(concatenateFields("item_id", "quantity")), 1),
        IndexTypes.SUM
    ));
    RecordMetaData rmd = rmdBuilder.getRecordMetaData();
    this.recordStoreBuilder = FDBRecordStore.newBuilder()
        .setMetaDataProvider(rmd)
        .setKeySpacePath(path);
    RecordStoreState storeState = fdb.run(cx -> {
      FDBRecordStore store = recordStoreBuilder.copyBuilder().setContext(cx).createOrOpen();
      return store.getRecordStoreState();
    });
    logger.info("storeState: " + storeState.toString());
    Set<String> disabledIndexNames = storeState.getDisabledIndexNames();
    logger.info("disabledIndexNames: " + Joiner.on(",").join(disabledIndexNames));
    AsyncUtil.whenAll(storeState.getDisabledIndexNames().stream()
        .map(indexName -> {
          // Build this index. It will begin the background job and return a future
          // that will complete when the index is ready for querying.
          OnlineIndexer indexBuilder = OnlineIndexer.newBuilder().setDatabase(fdb).
              setRecordStoreBuilder(recordStoreBuilder).setIndex(indexName).build();
          return indexBuilder.buildIndexAsync()
              .whenComplete((vignore, eignore) -> {
                logger.info("Finished Rebuilding Index: " + indexName);
                indexBuilder.close();
              });
        })
        .collect(Collectors.toList())
    ).join();
    writeRecords();
  }

  public void writeRecords() {
    fdb.run((FDBRecordContext cx) -> {
      FDBRecordStore store = recordStoreBuilder.copyBuilder().setContext(cx).createOrOpen();
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
    fdb.run((FDBRecordContext cx) -> {
      FDBRecordStore store = recordStoreBuilder.copyBuilder().setContext(cx).createOrOpen();

      store.saveRecord(SampleProto.Customer.newBuilder()
          .setCustomerId(9264L)
          .setFirstName("John")
          .setLastName("Smith")
          .addEmailAddress("jsmith@example.com")
          .addEmailAddress("john_smith@example.com")
          .addPreferenceTag("books")
          .addPreferenceTag("movies")
          .addOrder(
              SampleProto.Order.newBuilder()
                  .setOrderId(3875L)
                  .setItemId(9374L)
                  .setQuantity(2)
          )
          .addOrder(
              SampleProto.Order.newBuilder()
                  .setOrderId(4828L)
                  .setItemId(2740L)
                  .setQuantity(1)
          )
          .setPhoneNumber("(703) 555-8255")
          .build());

      store.saveRecord(SampleProto.Customer.newBuilder()
          .setCustomerId(8365L)
          .setFirstName("Jane")
          .setLastName("Doe")
          .addEmailAddress("jdoe@example.com")
          .addEmailAddress("jane_doe@example.com")
          .addPreferenceTag("games")
          .addPreferenceTag("lawn")
          .addPreferenceTag("books")
          .addOrder(
              SampleProto.Order.newBuilder()
                  .setOrderId(9280L)
                  .setItemId(2740L)
                  .setQuantity(3)
          )
          .setPhoneNumber("(408) 555-0248")
          .build());

      return null;
    });
  }
}
