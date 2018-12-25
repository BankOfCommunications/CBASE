if [ $# -lt 1 ]
then
  echo "Usage ./copy.sh \$oceanbase_dir [\$obconnector_dir]"
  OCEANBASE_DIR=`pwd`/../../
else
  OCEANBASE_DIR=$1
fi

if [ $# -gt 1 ]
then
  OBCONNECTOR_DIR=$2
fi

BIN_DIR=bin
LIB_DIR=lib
TOOLS_DIR=tools

#copy xxxserver
mkdir -p $BIN_DIR
cp $OCEANBASE_DIR/src/chunkserver/chunkserver $BIN_DIR/
echo 'cp '$OCEANBASE_DIR'/src/chunkserver/chunkserver' $BIN_DIR'/: '$?
cp $OCEANBASE_DIR/src/mergeserver/mergeserver $BIN_DIR/
echo 'cp '$OCEANBASE_DIR'/src/mergeserver/mergeserver' $BIN_DIR'/: '$?
cp $OCEANBASE_DIR/src/updateserver/updateserver $BIN_DIR/
echo 'cp '$OCEANBASE_DIR'/src/updateserver/updateserver' $BIN_DIR'/: '$?
cp $OCEANBASE_DIR/src/rootserver/rootserver $BIN_DIR
echo 'cp '$OCEANBASE_DIR'/src/rootserver/rootserver' $BIN_DIR'/: '$?
if [ $# -gt 1 ]
then
  cp $OBCONNECTOR_DIR/obconnector $BIN_DIR/proxyserver
  echo 'cp '$OBCONNECTOR_DIR'/obconnector' $BIN_DIR'/proxyserver:'$?
fi

#copy lib
mkdir -p $LIB_DIR
cp $OCEANBASE_DIR/src/common/compress/.libs/* $LIB_DIR/
echo 'cp '$OCEANBASE_DIR'/src/common/compress/.libs/lib' $LIB_DIR'/: '$?
cp $OCEANBASE_DIR/tools/sqltest/lib/libpq* $LIB_DIR/
echo cp $OCEANBASE_DIR/tools/sqltest/lib/libpq* $LIB_DIR/:$?
cp $OCEANBASE_DIR/tools/sqltest/lib/libmysqlclient* $LIB_DIR/
echo cp $OCEANBASE_DIR/tools/sqltest/lib/libmysqlclient* $LIB_DIR/:$?

#copy obsql lib
cp $OCEANBASE_DIR/src/client/obsql/src/.libs/libobsql* $LIB_DIR/
#cp /home/admin/yiming.czw/lib/libobsql.so.0.0.0 $LIB_DIR/
echo cp $OCEANBASE_DIR/src/client/obsql/src/.libs/libobsql* $LIB_DIR/:$?


#copy libobapi.so
#cp $OCEANBASE_DIR/src/client/cpp/.libs/libobapi.so* $LIB_DIR/
#echo cp $OCEANBASE_DIR/src/client/cpp/.libs/libobapi.so* $LIB_DIR/: $?

#copy tools
mkdir -p $TOOLS_DIR
cp $OCEANBASE_DIR/tools/ups_admin $TOOLS_DIR/
echo 'cp '$OCEANBASE_DIR'/tools/ups_admin' $TOOLS_DIR'/: '$?
cp $OCEANBASE_DIR/src/rootserver/rs_admin $TOOLS_DIR
echo 'cp '$OCEANBASE_DIR'/src/rootserver/rs_admin' $TOOLS_DIR'/: '$?
cp $OCEANBASE_DIR/tools/cs_admin $TOOLS_DIR/
echo 'cp '$OCEANBASE_DIR'/tools/cs_admin' $TOOLS_DIR'/: '$?
cp $OCEANBASE_DIR/tools/dumpsst $TOOLS_DIR/
echo 'cp '$OCEANBASE_DIR'/tools/dumpsst' $TOOLS_DIR'/: '$?
cp $OCEANBASE_DIR/tools/io_fault/iof $TOOLS_DIR/
echo 'cp '$OCEANBASE_DIR/tools/io_fault/iof $TOOLS_DIR'/: '$?
cp $OCEANBASE_DIR/tools/newsqltest/ob_tablet_join_test $TOOLS_DIR/
echo 'cp '$OCEANBASE_DIR/tools/newsqltest/ob_tablet_join_test $TOOLS_DIR'/: '$?

#copy syschecker/gen_sstable
mkdir -p syschecker
cp $OCEANBASE_DIR/tools/gen_sstable syschecker/
echo 'cp '$OCEANBASE_DIR'/tools/gen_sstable' syschecker'/: '$?

#copy benchmark/sysbench benchmark/ups_admin
cp $OCEANBASE_DIR/tools/ups_admin benchmark/
echo 'cp '$OCEANBASE_DIR'/tools/ups_admin' benchmark'/: '$?
# wget sysbench
wget "http://10.232.4.35:8877/sysbench" -O benchmark/sysbench && chmod +x benchmark/sysbench
echo "wget \"http://10.232.4.35:8877/sysbench\" -o benchmark/sysbench": $?
#copy trxtest/trxtest trxtest/ups_admin
cp $OCEANBASE_DIR/tools/ups_admin trxtest/
echo 'cp '$OCEANBASE_DIR'/tools/ups_admin' trxtest'/: '$?
# wget trxtest
wget "http://10.232.4.35:8877/trxtest" -O trxtest/trxtest && chmod +x trxtest/trxtest
echo "wget \"http://10.232.4.35:8877/trxtest\" -o trxtest/trxtest": $?

