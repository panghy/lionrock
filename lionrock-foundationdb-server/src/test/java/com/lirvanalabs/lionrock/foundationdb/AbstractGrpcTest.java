package com.lirvanalabs.lionrock.foundationdb;

import com.google.protobuf.ByteString;
import com.lirvanalabs.lionrock.proto.DatabaseResponse;
import com.lirvanalabs.lionrock.proto.KeySelector;
import com.lirvanalabs.lionrock.proto.StreamingDatabaseResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.lognet.springboot.grpc.GRpcServerRunner;
import org.lognet.springboot.grpc.autoconfigure.GRpcServerProperties;
import org.lognet.springboot.grpc.context.LocalRunningGrpcPort;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.Resource;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.NONE;

/**
 * Abstract class to handle basic test setup.
 *
 * @author Clement Pang
 */
@SpringBootTest(webEnvironment = NONE, properties = {"grpc.port=0", "grpc.shutdownGrace=-1"})
public abstract class AbstractGrpcTest {

  static Logger logger = LoggerFactory.getLogger(AbstractGrpcTest.class);
  static int FDB_PORT = 48064;

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

  private static final GenericContainer<?> fdb = new GenericContainer<>("foundationdb/foundationdb:6.2.30")
      .withNetworkMode("host")
      .withEnv("FDB_NETWORKING_MODE", "host")
      // pick a random port since we use host networking. FDB doesn't like NATing
      .withEnv("FDB_PORT", String.valueOf(FDB_PORT))
      .withLogConsumer(new Slf4jLogConsumer(logger))
      .waitingFor(Wait.forListeningPort());
  public static final String clusterFilePath = "./fdb.cluster";

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

  @BeforeAll
  public static void setupFoundationDbServer() throws InterruptedException, IOException {
    fdb.start();
    fdb.execInContainer("fdbcli", "--exec", "configure new single memory");

    String stdout;
    boolean fdbReady = false;

    // waiting for fdb to be up and healthy
    while (!fdbReady) {
      Container.ExecResult statusResult = fdb.execInContainer("fdbcli", "--exec", "status");
      stdout = statusResult.getStdout();

      if (stdout.contains("Healthy")) {
        fdbReady = true;
        logger.info("fdb is healthy");
      } else {
        logger.info("fdb is unhealthy");
        Thread.sleep(1000);
      }
    }

    createClusterFile(InetAddress.getByName(fdb.getContainerIpAddress()).getHostAddress(), FDB_PORT);
  }

  @AfterAll
  public static void shutdownFoundationDB() {
    logger.info("shutting down fdb");
    fdb.stop();
  }

  protected static void createClusterFile(String ip, Integer port) throws IOException {
    Path file = Paths.get(clusterFilePath);
    String content = "docker:docker@" + ip + ":" + port;
    Files.write(file, content.getBytes());
  }
}
