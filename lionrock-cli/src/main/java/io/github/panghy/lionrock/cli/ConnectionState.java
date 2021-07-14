package io.github.panghy.lionrock.cli;

import com.apple.foundationdb.Database;
import io.github.panghy.lionrock.client.foundationdb.RemoteFoundationDBDatabaseFactory;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStyle;
import org.springframework.shell.jline.PromptProvider;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;

@Component
public class ConnectionState implements PromptProvider {

  private ManagedChannel channel;

  private String prompt;

  public Database connect(String host, int port, String databaseIdentifier, String clientIdentifier) {
    if (channel != null) {
      channel.shutdownNow();
    }
    channel = ManagedChannelBuilder.forAddress(host, port).
        usePlaintext().
        build();
    prompt = host + ":" + port + ":" + databaseIdentifier;
    return RemoteFoundationDBDatabaseFactory.open(databaseIdentifier, clientIdentifier, channel);
  }

  @PreDestroy
  public void shutdown() {
    if (channel != null) {
      channel.shutdownNow();
    }
  }

  @Override
  public AttributedString getPrompt() {
    if (prompt == null) {
      return new AttributedString("not-connected:>",
          AttributedStyle.DEFAULT.foreground(AttributedStyle.RED));
    } else {
      return new AttributedString(prompt + ">",
          AttributedStyle.DEFAULT.foreground(
              channel.getState(false) == ConnectivityState.READY ?
                  AttributedStyle.GREEN :
                  AttributedStyle.YELLOW));
    }
  }
}
