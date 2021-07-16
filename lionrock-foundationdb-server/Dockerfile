ARG FDB_VERSION=6.3.15

FROM foundationdb/foundationdb:${FDB_VERSION} as fdb

FROM ghcr.io/graalvm/graalvm-ce:21.1.0

COPY --from=fdb /usr/lib/libfdb_c.so /usr/lib
COPY --from=fdb /usr/bin/fdbcli /usr/bin/
COPY --from=fdb /var/fdb/scripts/create_cluster_file.bash /bash/

ARG FDB_WEBSITE=https://www.foundationdb.org
ARG FDB_ADDITIONAL_VERSIONS=5.1.7
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