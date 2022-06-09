#!/bin/sh -e

# Start the service with systemd if it is available.
if pidof systemd > /dev/null; then
    # Use deb-systemd-invoke if available to respect policy-rc.d.
    systemctl=$(command -v deb-systemd-invoke || command -v systemctl)
    systemctl --system daemon-reload > /dev/null || true
    systemctl start foundationdb.service
else
    /etc/init.d/foundationdb start
fi

# Configure the database.
/usr/bin/fdbcli -C /etc/foundationdb/fdb.cluster --exec "configure new single memory; status" --timeout 20