#!/bin/sh
sudo sync
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
echo "Dropped cache"
sleep 0.3


