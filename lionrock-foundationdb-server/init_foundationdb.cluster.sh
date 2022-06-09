#!/bin/sh -e

description=`LC_CTYPE=C tr -dc A-Za-z0-9 < /dev/urandom | head -c 8`
random_str=`LC_CTYPE=C tr -dc A-Za-z0-9 < /dev/urandom | head -c 8`
echo $description:$random_str@127.0.0.1:4500 > /etc/foundationdb/fdb.cluster
chown foundationdb:foundationdb /etc/foundationdb/fdb.cluster
chmod 0664 /etc/foundationdb/fdb.cluster