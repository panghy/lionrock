FROM clementpang/foundationdb_built:7.1.6 as built_fdb

FROM adoptopenjdk/openjdk11:latest

RUN apt-get update && \
	apt-get install -y curl wget dnsutils lsof tcptraceroute telnet netcat strace tcpdump less vim net-tools jq && \
	rm -rf /var/lib/apt/lists/*

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