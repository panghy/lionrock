package io.github.panghy.lionrock.cli;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import io.github.panghy.lionrock.client.foundationdb.RemoteFoundationDBDatabaseFactory;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import javax.annotation.PreDestroy;
import java.io.ByteArrayOutputStream;

import static com.apple.foundationdb.tuple.ByteArrayUtil.printable;

@SpringBootApplication
@ShellComponent
public class LionrockCLI {

  private ManagedChannel channel;
  private Database db;
  private boolean writemode = false;

  @ShellMethod("Connect to a server.")
  public String connect(@ShellOption(value = "--host", defaultValue = "localhost") String host,
                        @ShellOption(value = "--port", defaultValue = "6565") int port,
                        @ShellOption(value = "--name", defaultValue = "fdb") String databaseName,
                        @ShellOption(value = "--identifier", defaultValue = "lionrock-cli") String clientIdentifier) {
    if (channel != null) {
      channel.shutdownNow();
    }
    channel = ManagedChannelBuilder.forAddress(host, port).
        usePlaintext().
        build();
    db = RemoteFoundationDBDatabaseFactory.open(databaseName, clientIdentifier, channel);
    return "connected to: " + host + ":" + port + " as: " + clientIdentifier + " accessing named database: " +
        databaseName;
  }

  @ShellMethod("Fetch the value for a given key.")
  public String get(@ShellOption(value = "--key") String key) {
    checkConnectionStatus();
    byte[] value = db.run(tx -> tx.get(fromPrintable(key)).join());
    if (value == null) {
      return "`" + key + "': not found";
    }
    return "`" + key + "' is `" + printable(value) + "'";
  }

  @ShellMethod("Enables or disables sets and clears.\n" +
      "\n" +
      "Setting or clearing keys from the CLI is not recommended.")
  public void writemode(@ShellOption(value = "--mode") OnOff mode) {
    checkConnectionStatus();
    writemode = mode == OnOff.on;
  }

  @ShellMethod("Set a value for a given key.")
  public String set(@ShellOption(value = "--key") String key,
                    @ShellOption(value = "--value") String value) {
    checkConnectionStatus();
    checkWriteMode();
    Transaction run = db.run(tx -> {
      tx.set(fromPrintable(key), fromPrintable(value));
      return tx;
    });
    return "Committed (" + run.getCommittedVersion() + ")";
  }

  @ShellMethod("Clear a key from the database.")
  public String clearkey(@ShellOption(value = "--key") String key) {
    checkConnectionStatus();
    checkWriteMode();
    Transaction run = db.run(tx -> {
      tx.clear(fromPrintable(key));
      return tx;
    });
    return "Committed (" + run.getCommittedVersion() + ")";
  }

  @ShellMethod("Clear a range of keys from the database.")
  public String clearrange(@ShellOption(value = "--start") String start,
                           @ShellOption(value = "--end") String end) {
    checkConnectionStatus();
    checkWriteMode();
    Transaction run = db.run(tx -> {
      tx.clear(fromPrintable(start), fromPrintable(end));
      return tx;
    });
    return "Committed (" + run.getCommittedVersion() + ")";
  }

  @ShellMethod("Fetch key/value pairs in a range of keys.")
  public String getrange(@ShellOption(value = "--start") String start,
                         @ShellOption(value = "--end", defaultValue = "\\xff") String end,
                         @ShellOption(value = "--limit", defaultValue = "25") int limit) {
    checkConnectionStatus();
    db.run(tx -> {
      System.out.println("\n Range limited to " + limit + " keys");
      for (KeyValue next : tx.getRange(fromPrintable(start), fromPrintable(end), limit)) {
        System.out.println("`" + printable(next.getKey()) + "' is `" + printable(next.getValue()) + "'");
      }
      return tx;
    });
    return "";
  }

  @ShellMethod("Watch the value of a key")
  public String watch(@ShellOption(value = "--key") String key) {
    checkConnectionStatus();
    Transaction run = db.run(tx -> {
      tx.watch(fromPrintable(key)).whenComplete((unused, throwable) -> {
        if (throwable != null) {
          System.out.println("\nwatch failed: " + throwable.getMessage());
        } else {
          System.out.println("\nwatch fired for key: " + key);
        }
      });
      return tx;
    });
    return "Committed (" + run.getCommittedVersion() + ")";
  }

  private void checkConnectionStatus() {
    if (db == null) {
      throw new IllegalStateException("must be connected to a remote server, type 'help connect' for instructions");
    }
  }

  private void checkWriteMode() {
    if (!writemode) {
      throw new IllegalStateException("ERROR: writemode must be enabled to set or clear keys in the database.");
    }
  }

  public static byte[] fromPrintable(String escaped) {
    ByteArrayOutputStream stream = new ByteArrayOutputStream(escaped.length());
    char[] chars = escaped.toCharArray();
    for (int i = 0; i < chars.length; i++) {
      char c = chars[i];
      if (c == '\\') {
        char next = chars[i + 1];
        if (next == '\\') {
          stream.write('\\');
          i += 1;
        } else if (next == 'x') {
          char first = chars[i + 2];
          char second = chars[i + 3];
          stream.write((byte) ((Character.digit(first, 16) << 4)
              + Character.digit(second, 16)));
          i += 3;
        } else {
          throw new IllegalArgumentException("malformed escape sequence");
        }
      } else {
        stream.write(c);
      }
    }
    return stream.toByteArray();
  }

  @PreDestroy
  private void shutdown() {
    if (db != null) {
      db.close();
    }
    if (channel != null) {
      channel.shutdownNow();
    }
  }

  public static void main(String[] args) {
    SpringApplication.run(LionrockCLI.class, args);
  }

  public enum OnOff {
    on,
    off;
  }
}