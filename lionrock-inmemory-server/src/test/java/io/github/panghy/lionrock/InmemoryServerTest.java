package io.github.panghy.lionrock;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.NONE;

@SpringBootTest(webEnvironment = NONE, properties = {"grpc.port=0", "grpc.shutdownGrace=0",
    "logging.level.io.github.panghy.lionrock=DEBUG"})
public class InmemoryServerTest {

  @Test
  public void contextLoads() {
  }
}
