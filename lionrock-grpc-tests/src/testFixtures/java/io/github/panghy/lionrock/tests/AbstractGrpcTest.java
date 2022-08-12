package io.github.panghy.lionrock.tests;

import com.google.protobuf.ByteString;
import io.github.panghy.lionrock.proto.DatabaseResponse;
import io.github.panghy.lionrock.proto.KeySelector;
import io.github.panghy.lionrock.proto.StreamingDatabaseResponse;
import io.github.panghy.lionrock.proto.TransactionalKeyValueStoreGrpc;
import io.grpc.StatusRuntimeException;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;

import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.NONE;

/**
 * Abstract class to handle basic test setup.
 *
 * @author Clement Pang
 */
@ComponentScan("io.github.panghy.lionrock")
@SpringBootTest(webEnvironment = NONE, properties = {
    "grpc.server.inProcessName=test", // Enable inProcess server
    "grpc.server.port=-1", // Disable external server
    "grpc.server.shutdown-grace-period=0", // Disable graceful shutdown
    "grpc.client.inProcess.address=in-process:test" // Configure the client to connect to the inProcess server
})
public abstract class AbstractGrpcTest {

  @Captor
  protected ArgumentCaptor<StatusRuntimeException> statusRuntimeExceptionArgumentCaptor;
  @Captor
  protected ArgumentCaptor<StreamingDatabaseResponse> streamingDatabaseResponseCapture;
  @Captor
  protected ArgumentCaptor<DatabaseResponse> databaseResponseCapture;

  @GrpcClient("inProcess")
  protected TransactionalKeyValueStoreGrpc.TransactionalKeyValueStoreStub stub;

  protected KeySelector keySelector(byte[] key, int offset, boolean orEqual) {
    return KeySelector.newBuilder().setKey(ByteString.copyFrom(key)).setOffset(offset).setOrEqual(orEqual).build();
  }

  protected KeySelector equals(byte[] key) {
    return KeySelector.newBuilder().setKey(ByteString.copyFrom(key)).setOffset(1).setOrEqual(false).build();
  }
}
