#!/bin/bash

POOL=sc19-test-michigan
CLIENT=$1
SLEEP=180 # 3 minutes
HOST=`hostname -s`

function create_cache {
 ceph osd pool create ${1}.cache 48 48 replicated sc19-ssd
 ceph osd tier add ${1} ${1}.cache
 ceph osd tier cache-mode ${1}.cache writeback
 ceph osd tier set-overlay ${1} ${1}.cache
 ceph osd pool set ${1}.cache hit_set_type bloom
 # 100GB
 ceph osd pool set ${1}.cache target_max_bytes 107374182400
 ceph osd pool set ${1}.cache target_max_bytes 107374182400
}

function remove_cache {
    ceph osd tier cache-mode ${1}.cache forward  --yes-i-really-mean-it
    rados -p ${1}.cache cache-flush-evict-all
    EMPTY=`rados -p ${1}.cache ls`
    if [ "$EMPTY" == '' ]; then
        ceph osd tier remove-overlay ${1}
        ceph osd tier remove ${1} ${1}.cache 
    else
        echo "Not removing cache pool ${1}.cache, not empty" | tee -a sc19-pool-control.txt
    fi
}

if [ "$CLIENT" == "--client" ]; then
    DATE=`date +%F-%H%M%S`
    for run in {1..10}; do
     echo "Benchmark run $run write"
     sudo rados -p ${POOL} bench 60 write --no-cleanup --concurrent-ios=16 2>$1 | tee sc19-rados-bench-write-${HOST}-run${run}-${DATE}.txt
     echo "Benchmark run $run read"
     sudo rados -p ${POOL} bench 60 seq --concurrent-ios=16 2>$1 | tee sc19-rados-bench-read-${HOST}-run${run}-${DATE}.txt
     echo "Benchmark run $run cleanup"
     sudo rados -p ${POOL} cleanup
    done

else

    echo "======== Starting new control run ========== " | tee -a  sc19-pool-control.txt
    for run in {1..10}; do
        DATE=`date +%F-%H%M%S`
        echo "Creating cache at $DATE" | tee -a sc19-pool-control.txt
        create_cache $POOL
        # 5 minutes 300 
        echo "Sleeping for $SLEEP seconds at $DATE" | tee -a sc19-pool-control.txt
        sleep $SLEEP
        echo "Initiating cache drain at $DATE" | tee -a sc19-pool-control.txt
        remove_cache $POOL
        echo "Finished cache drain at $DATE" | tee -a sc19-pool-control.txt
        sleep 10
    done
    echo "======== End of control run ========== " | tee -a  sc19-pool-control.txt
fi 
