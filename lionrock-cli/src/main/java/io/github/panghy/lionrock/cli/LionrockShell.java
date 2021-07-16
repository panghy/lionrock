package io.github.panghy.lionrock.cli;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import org.fusesource.jansi.AnsiConsole;
import org.jline.console.SystemRegistry;
import org.jline.console.impl.Builtins;
import org.jline.console.impl.SystemRegistryImpl;
import org.jline.keymap.KeyMap;
import org.jline.reader.*;
import org.jline.reader.impl.DefaultParser;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.widget.TailTipWidgets;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.shell.jline3.PicocliCommands;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Supplier;

import static com.apple.foundationdb.tuple.ByteArrayUtil.printable;
import static picocli.CommandLine.Spec.Target.MIXEE;

public class LionrockShell {

  private static final ConnectionState connectionState = new ConnectionState();
  private static Database db;
  private static boolean writemode = false;

  /**
   * Top-level command that just prints help.
   */
  @Command(name = "",
      description = {"Lionrock CLI Tool"},
      footer = {"", "Press Ctrl-D to exit."},
      subcommands = {
          SetValueCommand.class,
          ConnectCommand.class,
          SetWriteModeCommand.class,
          GetCommand.class,
          WatchCommand.class,
          GetRangeCommand.class,
          ClearRangeCommand.class,
          ClearValueCommand.class,
          CommandLine.HelpCommand.class})
  static class CliCommands implements Runnable {

    @CommandLine.Mixin
    LoggingMixin loggingMixin;

    PrintWriter out;

    CliCommands() {
    }

    public void run() {
      out.println(new CommandLine(this).getUsageMessage());
    }

    private Level calcLogLevel() {
      switch (loggingMixin.verbosity.length) {
        case 0:
          return Level.WARN;
        case 1:
          return Level.INFO;
        case 2:
          return Level.DEBUG;
        default:
          return Level.TRACE;
      }
    }

    // A reference to this method can be used as a custom execution strategy
    // that first configures Log4j based on the specified verbosity level,
    // and then delegates to the default execution strategy.
    private int executionStrategy(CommandLine.ParseResult parseResult) {
      Logger root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
      root.setLevel(calcLogLevel());
      return new CommandLine.RunLast().execute(parseResult); // default execution strategy
    }
  }

  @Command(name = "connect",
      mixinStandardHelpOptions = true,
      description = {"Connect to a server."})
  static class ConnectCommand implements Runnable {

    @CommandLine.Mixin
    LoggingMixin loggingMixin;

    @Parameters(defaultValue = "localhost")
    private String host;

    @Parameters(defaultValue = "6565")
    private int port;

    @Parameters(defaultValue = "fdb")
    private String databaseName;

    @Parameters(defaultValue = "lionrock-cli")
    private String clientIdentifier;

    public void run() {
      db = connectionState.connect(host, port, databaseName, clientIdentifier);
      System.out.println("connected to: " + host + ":" + port + " as: " + clientIdentifier +
          " accessing named database: " + databaseName);
    }
  }

  @Command(name = "get", mixinStandardHelpOptions = true,
      description = {"Fetch the value for a given key."})
  static class GetCommand implements Runnable {

    @CommandLine.Mixin
    LoggingMixin loggingMixin;

    @Parameters
    private String key;

    public void run() {
      checkConnectionStatus();
      byte[] value = db.run(tx -> tx.get(fromPrintable(key)).join());
      if (value == null) {
        System.out.println("`" + key + "': not found");
      } else {
        System.out.println("`" + key + "' is `" + printable(value) + "'");
      }
    }
  }

  @Command(name = "writemode",
      mixinStandardHelpOptions = true,
      description = {"Enables or disables sets and clears."})
  static class SetWriteModeCommand implements Runnable {

    @CommandLine.Mixin
    LoggingMixin loggingMixin;

    @Parameters
    private OnOff mode;

    public void run() {
      checkConnectionStatus();
      writemode = mode == OnOff.on;
    }
  }

  @Command(name = "set",
      mixinStandardHelpOptions = true,
      description = {"Set a value for a given key."})
  static class SetValueCommand implements Runnable {

    @CommandLine.Mixin
    LoggingMixin loggingMixin;

    @Parameters
    private String key;

    @Parameters
    private String value;

    public void run() {
      checkConnectionStatus();
      checkWriteMode();
      Transaction run = db.run(tx -> {
        tx.set(fromPrintable(key), fromPrintable(value));
        return tx;
      });
      System.out.println("Committed (" + run.getCommittedVersion() + ")");
    }
  }

  @Command(name = "clear",
      mixinStandardHelpOptions = true,
      description = {"Clear a key from the database."})
  static class ClearValueCommand implements Runnable {

    @CommandLine.Mixin
    LoggingMixin loggingMixin;

    @Parameters
    private String key;

    public void run() {
      checkConnectionStatus();
      checkWriteMode();
      Transaction run = db.run(tx -> {
        tx.clear(fromPrintable(key));
        return tx;
      });
      System.out.println("Committed (" + run.getCommittedVersion() + ")");
    }
  }

  @Command(name = "clearrange",
      mixinStandardHelpOptions = true,
      description = {"Clear a range of keys from the database."})
  static class ClearRangeCommand implements Runnable {

    @CommandLine.Mixin
    LoggingMixin loggingMixin;

    @Parameters
    private String start;

    @Parameters
    private String end;

    public void run() {
      checkConnectionStatus();
      checkWriteMode();
      Transaction run = db.run(tx -> {
        tx.clear(fromPrintable(start), fromPrintable(end));
        return tx;
      });
      System.out.println("Committed (" + run.getCommittedVersion() + ")");
    }
  }

  @Command(name = "getrange",
      mixinStandardHelpOptions = true,
      description = {"Fetch key/value pairs in a range of keys."})
  static class GetRangeCommand implements Runnable {

    @CommandLine.Mixin
    LoggingMixin loggingMixin;

    @Parameters
    private String start;

    @Parameters
    private String end;

    @Parameters(defaultValue = "25")
    private int limit;

    public void run() {
      checkConnectionStatus();
      db.run(tx -> {
        System.out.println("\n Range limited to " + limit + " keys");
        for (KeyValue next : tx.getRange(fromPrintable(start), fromPrintable(end), limit)) {
          System.out.println("`" + printable(next.getKey()) + "' is `" + printable(next.getValue()) + "'");
        }
        return tx;
      });
      System.out.println();
    }
  }

  @Command(name = "watch",
      mixinStandardHelpOptions = true,
      description = {"Watch the value of a key."})
  static class WatchCommand implements Runnable {

    @CommandLine.Mixin
    LoggingMixin loggingMixin;

    @Parameters
    private String key;

    public void run() {
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
      System.out.println("Committed (" + run.getCommittedVersion() + ")");
    }
  }

  static class LoggingMixin {
    private @CommandLine.Spec(MIXEE)
    CommandLine.Model.CommandSpec mixee; // spec of the command where the @Mixin is used

    boolean[] verbosity = new boolean[0];

    /**
     * Sets the specified verbosity on the LoggingMixin of the top-level command.
     *
     * @param verbosity the new verbosity value
     */
    @Option(names = {"-v", "--verbose"}, description = {
        "Specify multiple -v options to increase verbosity.",
        "For example, `-v -v -v` or `-vvv`"})
    public void setVerbose(boolean[] verbosity) {
      // Each subcommand that mixes in the LoggingMixin has its own instance
      // of this class, so there may be many LoggingMixin instances.
      // We want to store the verbosity value in a single, central place,
      // so we find the top-level command,
      // and store the verbosity level on our top-level command's LoggingMixin.
      ((CliCommands) mixee.root().userObject()).loggingMixin.verbosity = verbosity;
    }
  }

  static void checkConnectionStatus() {
    if (db == null) {
      throw new IllegalStateException("must be connected to a remote server, type 'connect -h' for instructions");
    }
  }

  static void checkWriteMode() {
    if (!writemode) {
      throw new IllegalStateException("ERROR: writemode must be enabled to set or clear keys in the database.");
    }
  }

  static byte[] fromPrintable(String escaped) {
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

  public static void main(String[] args) {
    AnsiConsole.systemInstall();
    try {
      Supplier<Path> workDir = () -> Paths.get(System.getProperty("user.dir"));
      // set up JLine built-in commands
      Builtins builtins = new Builtins(workDir, null, null);
      builtins.rename(Builtins.Command.TTOP, "top");
      builtins.alias("zle", "widget");
      builtins.alias("bindkey", "keymap");
      // set up picocli commands
      CliCommands commands = new CliCommands();

      PicocliCommands.PicocliCommandsFactory factory = new PicocliCommands.PicocliCommandsFactory();

      CommandLine cmd = new CommandLine(commands, factory).setExecutionStrategy(commands::executionStrategy);
      PicocliCommands picocliCommands = new PicocliCommands(cmd);

      DefaultParser parser = new DefaultParser();
      parser.setEscapeChars(new char[0]);
      try (Terminal terminal = TerminalBuilder.builder().jansi(true).jna(false).build()) {
        SystemRegistry systemRegistry = new SystemRegistryImpl(parser, terminal, workDir, null);
        systemRegistry.setCommandRegistries(builtins, picocliCommands);
        systemRegistry.register("help", picocliCommands);

        LineReader reader = LineReaderBuilder.builder()
            .terminal(terminal)
            .completer(systemRegistry.completer())
            .parser(parser)
            .variable(LineReader.LIST_MAX, 50)   // max tab completion candidates
            .build();
        builtins.setLineReader(reader);
        factory.setTerminal(terminal);
        TailTipWidgets widgets = new TailTipWidgets(reader, systemRegistry::commandDescription, 1,
            TailTipWidgets.TipType.COMPLETER);
        widgets.enable();
        KeyMap<Binding> keyMap = reader.getKeyMaps().get("main");
        keyMap.bind(new Reference("tailtip-toggle"), KeyMap.alt("s"));

        // start the shell and process input until the user quits with Ctrl-D
        String line;
        while (true) {
          try {
            systemRegistry.cleanUp();
            line = reader.readLine(connectionState.getPrompt().toAnsi(), null, (MaskingCallback) null, null);
            systemRegistry.execute(line);
          } catch (UserInterruptException e) {
            // Ignore
          } catch (EndOfFileException e) {
            return;
          } catch (Exception e) {
            systemRegistry.trace(e);
          }
        }
      }
    } catch (Throwable t) {
      t.printStackTrace();
    } finally {
      AnsiConsole.systemUninstall();
    }
  }

  public enum OnOff {
    on,
    off;
  }
}
