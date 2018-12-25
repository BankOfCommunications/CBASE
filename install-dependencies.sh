#!/bin/bash
sudo yum install lzo libaio-devel openssl-devel mysql-devel numactl-devel
sudo yum install -b test snappy t-csrd-tbnet-devel t_libeasy-devel tb-lua-dev t-db-congo-drcmessage
# install gtest
GTEST_SRC='http://googletest.googlecode.com/files/gtest-1.6.0.zip'
wget $GTEST_SRC -O gtest-1.6.0.zip
unzip ./gtest-1.6.0.zip
cd gtest-1.6.0
./configure
make 
sudo cp -rf include/* /usr/local/include/
sudo ./libtool --mode=install cp lib/libgtest.la /usr/local/lib/
cd ..
# install gmock
GMOCK_SRC='http://googlemock.googlecode.com/files/gmock-1.6.0.zip'
wget $GMOCK_SRC -O gmock-1.6.0.zip
unzip ./gmock-1.6.0.zip
cd gmock-1.6.0
./configure
make 
sudo cp -rf include/* /usr/local/include/
sudo ./libtool --mode=install cp lib/libgmock.la /usr/local/lib/
cd ..
# done
echo "Add the follow lines into your .bashrc:"
echo -e "\nexport TBLIB_ROOT=/opt/csr/common\nexport EASY_ROOT=/usr\nexport EASY_LIB_PATH=/usr/lib64\nexport DRC_ROOT=/home/ds\nexport LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:/home/ds/lib64/"
