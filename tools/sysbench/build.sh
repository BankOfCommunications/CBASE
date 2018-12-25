app=$1
if [ -z $app ];then
    echo "./build.sh sysbench or  ./build.sh trxtest"
    exit 0
fi
if [ "z$app" = "zsysbench" ];then
  #./configure --with-oltp CC=g++ CPPFLAGS=-O0   --with-mysql-includes=/home/xiaojun.chengxj/mysql/include/ --with-mysql-libs=/home/xiaojun.chengxj/mysql/lib/
  ./configure --with-oltp CC=g++ CPPFLAGS=-O0  
else
  ./configure CC=g++ CPPFLAGS=-O0  
fi
make -j

  
