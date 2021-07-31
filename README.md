# lionrock [![Gradle Build](https://github.com/panghy/lionrock/actions/workflows/gradle.yml/badge.svg)](https://github.com/panghy/lionrock/actions/workflows/gradle.yml) [![Code Coverage](https://raw.githubusercontent.com/panghy/lionrock/master/.github/badges/jacoco.svg)](https://raw.githubusercontent.com/panghy/lionrock/master/.github/badges/jacoco.svg) [![Branch Coverage](https://raw.githubusercontent.com/panghy/lionrock/master/.github/badges/branches.svg)](https://raw.githubusercontent.com/panghy/lionrock/master/.github/badges/branches.svg)

An implementation agnostic client/server communication protocol (using protobuf and grpc) inspired heavily by
FoundationDB (https://github.com/apple/foundationdb).

## Features

* An implementation of the server (lionrock-foundationdb-server) that's backed by an actual FoundationDB Cluster
* Could be used as a facade layer in front of a FoundationDB Server without having to utilize native libraries
* An API-compatible (with caveats) drop-in replacement of the FoundationDB Java Client is provided that talks to the
  gRPC server instead of using native libraries
* Other backends could be wired up in the future (for instance, an in-memory server could be used for local development
  or tests)
* [Proto](https://github.com/panghy/lionrock/blob/master/proto/lionrock.proto) file that allows any supported language
  by gRPC to connect to the FoundationDB facade

## Known Issues

* Transaction lifecycling is handled by a single gRPC server (that is stateless and can run in replicated
  configurations), as such, operations such as watches can fail when a single gRPC facade server is restarted
* Each call to a transaction is streamed back to the server, future work to batch up mutations can be implemented for
  better performance
* Locality APIs will not work against a RemoteTransaction (which is not an FDBTransaction), a RemoteLocalityUtil
  implementation is provided in lionrock-foundationdb-client)
* Because the Java client still depends on fdb-java, it is possible that the native libraries will still be loaded (and
  might fail on unsupported platforms)
* Support libraries such as Tuples are still provided in fdb-java library

## Building

### Requirements

* JDK 11 or higher
* FoundationDB Server/Client binaries installed on the machine

### How to Build and Run

To launch your tests:

```
./gradlew clean test
```

To run the server on the default port:

```
./gradlew :lionrock-foundationdb-server:bootRun --args='--management.metrics.export.wavefront.enabled=true'
```

To run the server with micrometer and sleuth reporting to Wavefront (with a throw away account by default):

```
./gradlew :lionrock-foundationdb-server:bootRun --args='--management.metrics.export.wavefront.enabled=true'
```

```
A Wavefront account has been provisioned successfully and the API token has been saved to disk.

To share this account, make sure the following is added to your configuration:

        management.metrics.export.wavefront.api-token=xxxxxxx
        management.metrics.export.wavefront.uri=https://wavefront.surf

Connect to your Wavefront dashboard using this one-time use link:
https://wavefront.surf/us/xxxxxxx
```

By clicking on the link, you can see metrics and traces from the server (depending on sampling etc.)

To use an actual Wavefront account:

```
./gradlew :lionrock-foundationdb-server:bootRun --args='--management.metrics.export.wavefront.enabled=true,management.metrics.export.wavefront.api-token=xxxxxxx,management.metrics.export.wavefront.uri=https://wavefront.surf'
```

Or use a YAML file (see Spring Boot documentation) for less command-line mangling.

# FoundationDB gRPC Facade

## Configuration

By default, the cluster named "fdb" is used to identify the intended cluster to use from a request. It is possible to
expose multiple FoundationDB clusters by mapping a name to cluster files on disk.

```yaml
lionrock:
  foundationdb:
    clusters:
      # Default cluster
      - name: fdb
      - name: another-fdb-cluster
        clusterFile: /etc/foundationdb/another-fdb-cluster.cluster
```

## Logging

You can enable detailed logging of all requests (with trace and span IDs) by enabling DEBUG logging in the facade:

```
./gradlew :lionrock-foundationdb-server:bootRun --args='--logging.level.io.github.panghy.lionrock=DEBUG'
```

Sample log output:

```
2021-07-12 12:28:52.964 DEBUG [fdb-facade,60ec97f43a2e0f97d084b377b042fa3e,d084b377b042fa3e] 9936 --- [ault-executor-1] i.g.p.l.f.FoundationDbGrpcFacade         : WatchKeyRequest for: hello
2021-07-12 12:28:52.965 DEBUG [fdb-facade,60ec97f43a2e0f97d084b377b042fa3e,d084b377b042fa3e] 9936 --- [ault-executor-1] i.g.p.l.f.FoundationDbGrpcFacade         : CommitTransactionRequest
2021-07-12 12:28:52.966 DEBUG [fdb-facade,60ec97f43a2e0f97d084b377b042fa3e,d2f415921f110513] 9936 --- [     fdb-java-2] i.g.p.l.f.FoundationDbGrpcFacade         : Committed transaction: -1
2021-07-12 12:28:52.977 DEBUG [fdb-facade,60ec97f4e6736daac6d369ecc2fa2bb6,c6d369ecc2fa2bb6] 9936 --- [ault-executor-0] i.g.p.l.f.FoundationDbGrpcFacade         : Starting transaction setKeyAndCommit against db: fdb
2021-07-12 12:28:52.977 DEBUG [fdb-facade,60ec97f4e6736daac6d369ecc2fa2bb6,c6d369ecc2fa2bb6] 9936 --- [ault-executor-0] i.g.p.l.f.FoundationDbGrpcFacade         : SetValueRequest for: hello => 
2021-07-12 12:28:52.977 DEBUG [fdb-facade,60ec97f4e6736daac6d369ecc2fa2bb6,c6d369ecc2fa2bb6] 9936 --- [ault-executor-0] i.g.p.l.f.FoundationDbGrpcFacade         : GetApproximateSizeRequest
2021-07-12 12:28:52.977 DEBUG [fdb-facade,60ec97f4e6736daac6d369ecc2fa2bb6,d5ed2a246b6fcc6c] 9936 --- [     fdb-java-2] i.g.p.l.f.FoundationDbGrpcFacade         : GetApproximateSize is: 68
2021-07-12 12:28:52.988 DEBUG [fdb-facade,60ec97f4e6736daac6d369ecc2fa2bb6,c6d369ecc2fa2bb6] 9936 --- [ault-executor-0] i.g.p.l.f.FoundationDbGrpcFacade         : CommitTransactionRequest
2021-07-12 12:28:52.997 DEBUG [fdb-facade,60ec97f4e6736daac6d369ecc2fa2bb6,2693bc53fb9b27dd] 9936 --- [     fdb-java-2] i.g.p.l.f.FoundationDbGrpcFacade         : Committed transaction: 105114348668435
2021-07-12 12:28:53.002 DEBUG [fdb-facade,60ec97f43a2e0f97d084b377b042fa3e,5e028e13152da40f] 9936 --- [     fdb-java-2] i.g.p.l.f.FoundationDbGrpcFacade         : WatchKeyRequest Completed for: hello
```

## Deploying on Kubernetes

Assuming that you have a cluster that's setup with the Foundationdb Operator:

See https://github.com/panghy/lionrock/blob/master/lionrock-foundationdb-server/release/lionrock.yaml for an example on
how you can run the server.

You need to replace `<YOUR_CLUSTER_NAME>` with the name of your cluster when it was setup.

# FoundationDB gRPC Client

This is a drop-in client for code that otherwise would be using the original java foundationdb library (fdb-java). It
depends on that library for the interface API as well as supporting libraries like Tuple.

```xml

<dependency>
  <groupId>io.github.panghy.lionrock</groupId>
  <artifactId>lionrock-foundationdb-client</artifactId>
  <version>1.0.0</version>
</dependency>
```

Note that you could use the client with any server that implements the lionrock protocol.

```java
public class FdbClient {
  public static void main(String[] args) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 6565).
            usePlaintext().
            build();
    // "fdb" is the default database on the server.
    Database db = RemoteFoundationDBDatabaseFactory.open("fdb", "my client", channel);
    db.runAsync(tx -> {
      tx.set(HELLO_B, WORLD_B);
      return tx.get(HELLO_B);
    }).join();
    channel.shutdown();
  }
}
```

# Lionrock CLI Client

```
./gradlew :lionrock-cli:shadow && java -jar lionrock-cli/build/libs/lionrock-cli-0.0.1-SNAPSHOT-all.jar
```

GraalVM native-image:

```
./gradlew :lionrock-cli:nativeImage && lionrock-cli/build/bin/lionrock-cli
```

Use the command `connect` to connect to a remote server (defaults to localhost:6565).

# Lionrock Test Server

Run a self-contained Lionrock server (internally backed by FDB) to try out the server.

```shell
docker run -p 6565:6565 clementpang/lionrock-test-server:latest
```

Build (see above) or download the CLI from the release page and connect to it.

![CLI Demo](https://github.com/panghy/lionrock/blob/master/.github/images/cli-demo2.gif?raw=true "CLI Demo")

# Streaming gRPC Connection Lifecycle

Running FoundationDB transactions over a streaming gRPC connection presents unique challenges when compared to the
native library version of the client. This is due to the fact that a single transaction is masqueraded as a
bi-directional streaming RPC request between a client and a server. The question at hand is a) when should a client
terminate the streaming connection to the server, b) when should the server terminate the streaming connection back to
the client, and c) what to do to the many CompletableFutures that the client generates when the connection fails (I/O
exceptions or otherwise).

The API for FoundationDB allows the user to interact with Transaction objects even after the transaction commits
(getVersionstamp() or getCommittedVersion() comes to mind). Watches also survive longer than even when the Transaction
is explicitly closed() (implying that resources w.r.t. the Transaction should be reclaimed). All such properties mean
that we cannot close the connection from the server to the client when we observe a commit. We can't also close the
connection when close() is explicitly called on the client-side Transaction either.

Currently, the design of connection lifecycling is such that the client takes the initiative on closing the connection
(server timeouts not-withstanding), when all watches have been resolved and the client-side Transaction is closed(), we
assume no additional operation is possible against the transaction context and at that point, we let the server know
that the connection can be closed (gRPC multiplexes requests on a single connection so the cost of maintaining many open
connections is not as significant as one would think).

When a connection error occurs (or when the transaction itself errors out; we fail the gRPC request entirely when
transaction commit fails), we need to make sure that all outstanding client-side CompletableFutures are marked as
exceptionally completed. Individual asynchronous calls can error out on their own (which is sent as a stream message
back to the client) but when the entire request is no longer feasible (and hence no stream messages would be received
from that point forward), we need to make sure that there are no dangling futures.

We also introduce an error when the server closes the connection (perhaps there was an intermediate proxy?) without
satisfying all client-side outstanding CompletableFutures. We mark those with an internal error designation with the
message "server_left".