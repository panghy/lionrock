ARG FDB_VERSION=6.3.15

FROM foundationdb/foundationdb:${FDB_VERSION} as fdb

FROM adoptopenjdk/openjdk11:latest

RUN apt-get update && \
	apt-get install -y curl>=7.58.0-2ubuntu3.6 \
		dnsutils>=1:9.11.3+dfsg-1ubuntu1.7 \
		lsof>=4.89+dfsg-0.1 \
		tcptraceroute>=1.5beta7+debian-4build1 \
		telnet>=0.17-41 \
		netcat>=1.10-41.1 \
		strace>=4.21-1ubuntu1 \
		tcpdump>=4.9.3-0ubuntu0.18.04.1 \
		less>=487-0.1 \
		vim>=2:8.0.1453-1ubuntu1.4 \
		net-tools>=1.60+git20161116.90da8a0-1ubuntu1 \
		jq>=1.5+dfsg-2 && \
	rm -rf /var/lib/apt/lists/*

COPY --from=fdb /usr/lib/libfdb_c.so /usr/lib
COPY --from=fdb /usr/bin/fdbcli /usr/bin/
COPY --from=fdb /var/fdb/scripts/create_cluster_file.bash /bash/

ARG FDB_WEBSITE=https://www.foundationdb.org
ARG FDB_ADDITIONAL_VERSIONS="5.1.7 6.2.30"
ENV FDB_NETWORK_OPTION_EXTERNAL_CLIENT_DIRECTORY=/usr/lib/fdb-multiversion
RUN mkdir /usr/lib/fdb-multiversion; \
	for version in ${FDB_ADDITIONAL_VERSIONS}; do \
		curl ${FDB_WEBSITE}/downloads/$version/linux/libfdb_c_$version.so -o /usr/lib/fdb-multiversion/libfdb_c_$version.so; \
	done

RUN ldconfig

COPY release/start.bash /bash
RUN chmod a+x /bash/start.bash

WORKDIR /

ARG JAR_FILE=build/libs/lionrock-foundationdb-server-*.*.*-boot.jar
COPY ${JAR_FILE} app.jar

EXPOSE 6565

CMD /bash/start.bash