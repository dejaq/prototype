#!/usr/bin/env bash


#time is a sensitive manner for dejaq, we need ms time sync, also for cockroachDB
#TODO this does not work yet, fix it https://developers.google.com/time/guides
#for now ntp uses 0.amazon.pool.ntp.org should be fine
# it also has this feature https://developers.google.com/time/smear
sudo timedatectl set-ntp no
sudo yum -y -q upgrade
sudo yum -y -q install ntp
sudo service ntp stop
sudo ntpd -b time.google.com
sudo service ntp start
sudo ntpq -p