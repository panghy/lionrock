ARG FDB_VERSION=6.3.24

FROM foundationdb/foundationdb:${FDB_VERSION} as fdb

FROM clementpang/lionrock-foundationdb-base:latest

COPY --from=fdb /usr/lib/libfdb_c.so /usr/lib
COPY --from=fdb /usr/bin/fdbcli /usr/bin/

ARG FDB_WEBSITE=https://github.com/apple/foundationdb/releases/downloads
ARG FDB_ADDITIONAL_VERSIONS="5.1.7 6.2.30"
ENV FDB_NETWORK_OPTION_EXTERNAL_CLIENT_DIRECTORY=/usr/lib/fdb-multiversion
RUN mkdir /usr/lib/fdb-multiversion; \
	for version in ${FDB_ADDITIONAL_VERSIONS}; do \
		curl ${FDB_WEBSITE}/$version/libfdb_c.x86_64.so -o /usr/lib/fdb-multiversion/libfdb_c_$version.so; \
	done

RUN ldconfig

COPY release/start.bash /bash/start.bash
RUN chmod a+x /bash/start.bash

WORKDIR /

ARG JAR_FILE=build/libs/lionrock-foundationdb-server-*.*.*-boot.jar
COPY ${JAR_FILE} app.jar

EXPOSE 6565

CMD /bash/start.bash