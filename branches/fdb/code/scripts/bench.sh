#!/bin/sh

clearcache() {
	sudo sync
	sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
	echo "Dropped cache"
	sleep 0.3
}

# rm -f fdbmain.log
# rm -f ../../data/*.db
# rm -f ../../data/data.sig
# clearcache
# ./fdbmain loadssbmv
# clearcache
# ./fdbmain loadssbmvcstore
clearcache
./build/fdbmain runbench 1000 false false 10 400000 40
clearcache
./build/fdbmain runbench 1000 false true 10 400000 40
clearcache
./build/fdbmain runbench 1000 true false 10 400000 40
clearcache
./build/fdbmain runbench 1000 true true 10 400000 40
clearcache
./build/fdbmain runbench 1000 false false 10 0 40
clearcache
./build/fdbmain runbench 1000 true false 10 0 40
clearcache

