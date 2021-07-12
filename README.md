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
* Locality APIs will not work against a RemoteTransaction (which is not an FDBTransaction)
* No CLI is available for the server (yet)
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
Please note that by default it'll negotiate a throw away account for monitoring the service via Wavefront

```
./gradlew bootRun --args='--management.metrics.export.wavefront.enabled=true'
```

To run the server with micrometer and sleuth reporting to Wavefront (with a throw away account by default):

```
./gradlew bootRun --args='--management.metrics.export.wavefront.enabled=true'
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
./gradlew bootRun --args='--management.metrics.export.wavefront.enabled=true,management.metrics.export.wavefront.api-token=xxxxxxx,management.metrics.export.wavefront.uri=https://wavefront.surf'
```

Or use a YAML file (configuration below):

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