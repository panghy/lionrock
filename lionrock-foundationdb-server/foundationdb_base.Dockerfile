FROM adoptopenjdk/openjdk11:latest

# Per https://github.com/docker/buildx/issues/495#issuecomment-995503425
RUN ln -s /usr/bin/dpkg-split /usr/sbin/dpkg-split
RUN ln -s /usr/bin/dpkg-deb /usr/sbin/dpkg-deb
RUN ln -s /bin/rm /usr/sbin/rm
RUN ln -s /bin/tar /usr/sbin/tar

RUN apt-get upgrade -y && \
    apt-get update && \
	apt-get install -y curl wget dnsutils lsof tcptraceroute telnet netcat strace tcpdump less vim net-tools jq && \
	rm -rf /var/lib/apt/lists/*