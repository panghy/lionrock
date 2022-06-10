ARG FDB_VERSION=6.3.24

FROM clementpang/foundationdb_build:latest as scratch
ARG FDB_VERSION

WORKDIR /
RUN mkdir fdb-src
WORKDIR /fdb-src
RUN git clone https://github.com/apple/foundationdb.git
WORKDIR /fdb-src/foundationdb
RUN echo $FDB_VERSION
RUN git checkout $FDB_VERSION

WORKDIR /
RUN mkdir fdb-build
WORKDIR /fdb-build
RUN cmake -D CMAKE_CXX_FLAGS="-U_FORTIFY_SOURCE" -G Ninja /fdb-src/foundationdb
RUN ninja
RUN cpack -G DEB

FROM ubuntu:latest

COPY --from=scratch /fdb-build/packages/ /fdb-build/packages/