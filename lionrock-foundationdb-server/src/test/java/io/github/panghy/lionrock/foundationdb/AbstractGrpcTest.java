package io.github.panghy.lionrock.foundationdb;

import com.google.protobuf.ByteString;
import io.github.panghy.lionrock.proto.DatabaseResponse;
import io.github.panghy.lionrock.proto.KeySelector;
import io.github.panghy.lionrock.proto.StreamingDatabaseResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.lognet.springboot.grpc.GRpcServerRunner;
import org.lognet.springboot.grpc.autoconfigure.GRpcServerProperties;
import org.lognet.springboot.grpc.context.LocalRunningGrpcPort;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.util.Optional;

import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.NONE;

/**
 * Abstract class to handle basic test setup.
 *
 * @author Clement Pang
 */
@SpringBootTest(webEnvironment = NONE, properties = {"grpc.port=0", "grpc.shutdownGrace=-1"})
public abstract class AbstractGrpcTest {
  @Autowired(required = false)
  @Qualifier("grpcServerRunner")
  protected GRpcServerRunner grpcServerRunner;

  @Autowired
  protected GRpcServerProperties gRpcServerProperties;

  @LocalRunningGrpcPort
  protected int runningPort;

  @Captor
  ArgumentCaptor<StatusRuntimeException> statusRuntimeExceptionArgumentCaptor;
  @Captor
  ArgumentCaptor<StreamingDatabaseResponse> streamingDatabaseResponseCapture;
  @Captor
  ArgumentCaptor<DatabaseResponse> databaseResponseCapture;

  protected ManagedChannel channel;

  @BeforeEach
  public void setupChannel() throws IOException {
    if (gRpcServerProperties.isEnabled()) {
      ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress("localhost", getPort());
      Resource certChain = Optional.ofNullable(gRpcServerProperties.getSecurity()).
          map(GRpcServerProperties.SecurityProperties::getCertChain).
          orElse(null);
      if (null != certChain) {
        ((NettyChannelBuilder) channelBuilder).
            useTransportSecurity().
            sslContext(GrpcSslContexts.forClient().trustManager(certChain.getInputStream()).build());
      } else {
        channelBuilder.usePlaintext();
      }
      channel = channelBuilder.build();
    }
  }

  @AfterEach
  public void shutdownChannel() {
    Optional.ofNullable(channel).ifPresent(ManagedChannel::shutdownNow);
  }

  protected int getPort() {
    return runningPort;
  }

  KeySelector keySelector(byte[] key, int offset, boolean orEqual) {
    return KeySelector.newBuilder().setKey(ByteString.copyFrom(key)).setOffset(offset).setOrEqual(orEqual).build();
  }

  KeySelector equals(byte[] key) {
    return KeySelector.newBuilder().setKey(ByteString.copyFrom(key)).setOffset(1).setOrEqual(false).build();
  }
}
