#!/bin/sh

if [ $# != 3 ]; then
    echo "Usage: $0 <grouphost> 'source_full_path_file' 'full_path_file'";
    exit 1;
fi

##### HOMEĿ¼ ###############
#REAL_FILE=`readlink -f $0`
#cd `dirname $REAL_FILE`/..
##############################
BASE_HOME=`dirname $0`
host=`$BASE_HOME/gethost.sh $1`

for DEST_HOST in $host
do
    echo -e "\033[32;1m========================== $DEST_HOST ==========================\033[0m";
    scp -r -p $2 admin@$DEST_HOST:$3
done

echo -e "\033[32;1m========================= All done!!! ======================\033[0m";
