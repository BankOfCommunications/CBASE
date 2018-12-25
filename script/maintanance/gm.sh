#!/bin/bash

if [ $# != 2 ]; then
    echo "Usage: $0 hostname 'command'";
    exit 1;
fi

##### HOMEĿ¼ ###############
#REAL_FILE=`readlink -f $0`
#cd `dirname $REAL_FILE`/..
#BASE_HOME="`pwd`"
##############################
BASE_HOME=`dirname $0`
host=`$BASE_HOME/gethost.sh $1`

for DEST_HOST in $host 
do
    echo -e "\033[32;1m=========================== $DEST_HOST ===========================\033[0m";
    ssh -o ConnectTimeout=1 admin@$DEST_HOST "$2" 
    echo
done

echo "=== All done!!! ==="

