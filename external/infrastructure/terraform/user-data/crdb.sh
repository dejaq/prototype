#!/usr/bin/env bash


# set hostname
hostname "${host_name}"

# https://www.cockroachlabs.com/docs/stable/deploy-cockroachdb-on-premises-insecure.html
#TODO install it as a daemon
wget -qO- https://binaries.cockroachdb.com/cockroach-v19.2.6.linux-amd64.tgz | tar  xvz
cp -i cockroach-v19.2.6.linux-amd64/cockroach /usr/local/bin/


#export CPU and other resources for Prometheus
# https://www.cockroachlabs.com/docs/stable/monitoring-and-alerting.html#prometheus-endpoint