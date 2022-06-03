FROM clementpang/foundationdb_build:latest

WORKDIR /
RUN mkdir fdb-src
WORKDIR /fdb-src
RUN git clone https://github.com/apple/foundationdb.git
WORKDIR /fdb-src/foundationdb
RUN git checkout 6.3.24

WORKDIR /
RUN mkdir fdb-build
WORKDIR /fdb-build
RUN apt install -y default-jdk
RUN cmake -D CMAKE_CXX_FLAGS="-U_FORTIFY_SOURCE  " -G Ninja /fdb-src/foundationdb
RUN ninja
RUN cpack -G DEB