#!/bin/sh

OBAPI_VERSION="1.1.0"
TMP_DIR=libobapi.tmp.$$
mkdir $TMP_DIR
cp oceanbase.h $TMP_DIR
cp -P .libs/libobapi.so* $TMP_DIR

cd $TMP_DIR
tar czf libobapi-$OBAPI_VERSION.tar.gz *
mv libobapi-$OBAPI_VERSION.tar.gz ..

cd ..

rm -r $TMP_DIR

