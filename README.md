# lionrock [![Gradle Build](https://github.com/panghy/lionrock/actions/workflows/gradle.yml/badge.svg)](https://github.com/panghy/lionrock/actions/workflows/gradle.yml) [![Code Coverage](https://raw.githubusercontent.com/panghy/lionrock/master/.github/badges/jacoco.svg)](https://raw.githubusercontent.com/panghy/lionrock/master/.github/badges/jacoco.svg) [![Branch Coverage](https://raw.githubusercontent.com/panghy/lionrock/master/.github/badges/branches.svg)](https://raw.githubusercontent.com/panghy/lionrock/master/.github/badges/branches.svg)

An implementation agnostic client/server communication protocol (using protobuf and grpc) inspired heavily by
FoundationDB (https://github.com/apple/foundationdb).

## Features

* An implementation of the server (lionrock-foundationdb-server) that's backed by an actual FoundationDB Cluster using
  Spring Boot
* Could be used as a facade layer in front of a FoundationDB Server without having to utilize native libraries
* [Proto](https://github.com/panghy/lionrock/blob/master/proto/lionrock.proto) file that allows any supported language
  by gRPC to connect to the FoundationDB facade

## Building

### Requirements

* JDK 11 or more
* FoundationDB Server/Client binaries installed on the machine

### Gradle cheat-sheet

To launch your tests:

```
./gradlew clean test
```

To package your application:

```
./gradlew clean assemble
```

To run your application:

```
./gradlew clean run
```