package io.github.panghy.lionrock.cli;

import com.apple.foundationdb.Database;
import io.github.panghy.lionrock.client.foundationdb.RemoteFoundationDBDatabaseFactory;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStyle;

public class ConnectionState {

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

  public AttributedString getPrompt() {
    if (prompt == null) {
      return new AttributedString("not-connected:>",
          AttributedStyle.DEFAULT.foreground(AttributedStyle.RED));
    } else {
      ConnectivityState state = channel.getState(false);
      return new AttributedString(prompt + ":" + state + ">",
          AttributedStyle.DEFAULT.foreground(
              state == ConnectivityState.READY ? AttributedStyle.GREEN : AttributedStyle.YELLOW));
    }
  }
}
