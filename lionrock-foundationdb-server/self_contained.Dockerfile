FROM adoptopenjdk/openjdk11:latest

RUN apt-get update && \
	apt-get install -y curl>=7.58.0-2ubuntu3.6 \
	    wget \
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

WORKDIR /

RUN wget https://www.foundationdb.org/downloads/6.3.15/ubuntu/installers/foundationdb-clients_6.3.15-1_amd64.deb
RUN wget https://www.foundationdb.org/downloads/6.3.15/ubuntu/installers/foundationdb-server_6.3.15-1_amd64.deb
RUN dpkg -i foundationdb-clients_6.3.15-1_amd64.deb
RUN dpkg -i foundationdb-server_6.3.15-1_amd64.deb
RUN rm foundationdb-clients_6.3.15-1_amd64.deb
RUN rm foundationdb-server_6.3.15-1_amd64.deb

RUN mkdir -p /var/fdb/logs

WORKDIR /

ARG JAR_FILE=build/libs/lionrock-foundationdb-server-*.*.*-boot.jar
COPY ${JAR_FILE} app.jar

EXPOSE 6565

CMD service foundationdb start; java -jar app.jar