#!/bin/sh

TMP_DIR="__tmp$$"
PWD_DIR=`pwd`

mkdir -p $TMP_DIR

cp .libs/libobapi.a $TMP_DIR
cp ../../common/libcommon.a $TMP_DIR
cp ../../rootserver/librootserver.a $TMP_DIR
cp $TBLIB_ROOT/lib/libtbsys.a $TMP_DIR
cp $TBLIB_ROOT/lib/libtbnet.a $TMP_DIR

cd $TMP_DIR

ar x libobapi.a
ar x libcommon.a
ar x librootserver.a
ar x libtbsys.a
ar x libtbnet.a

ar cru $PWD_DIR/libobapi.a *.o
ranlib $PWD_DIR/libobapi.a

cd $PWD_DIR
rm -r $TMP_DIR

