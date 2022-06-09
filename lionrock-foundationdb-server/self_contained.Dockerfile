ARG FDB_VERSION=6.3.24

FROM clementpang/foundationdb_binaries:${FDB_VERSION} as built_fdb

FROM clementpang/lionrock-foundationdb-base:latest

WORKDIR /

COPY --from=built_fdb /fdb-build/packages/foundationdb-clients*.deb foundationdb-clients.deb
COPY --from=built_fdb /fdb-build/packages/foundationdb-server*.deb foundationdb-server.deb
COPY --from=built_fdb /fdb-build/packages/lib/libfdb_java.so libfdb_java.so
RUN dpkg -i foundationdb-clients.deb
RUN dpkg -i foundationdb-server.deb
RUN rm foundationdb-clients.deb
RUN rm foundationdb-server.deb

RUN mkdir -p /var/fdb/logs

WORKDIR /

ARG JAR_FILE=build/libs/lionrock-foundationdb-server-*.*.*-boot.jar
COPY ${JAR_FILE} app.jar

EXPOSE 6565

CMD service foundationdb start; java -DFDB_LIBRARY_PATH_FDB_JAVA=/libfdb_java.so -jar app.jar