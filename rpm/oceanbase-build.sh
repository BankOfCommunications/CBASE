#!/bin/bash
#for taobao abs
# Usage: oceanbase-build.sh <oceanbasepath> <package> <version> <release>
# Usage: oceanbase-build.sh

REDHAT=`cat /etc/redhat-release|cut -d " " -f 7|cut -d "." -f 1`

if [ $# -ne 4 ]
then

	TOP_DIR=`pwd`/../
	PACKAGE=oceanbase
	VERSION=`cat oceanbase-VER.txt`
	RELEASE="bankcomm_v0.07"
	SPEC_FILE=oceanbase_test.spec
else
	TOP_DIR=$1
	PACKAGE=$2
	VERSION=$3
	RELEASE="$4.el${REDHAT}"
	SPEC_FILE=oceanbase.spec
	TBLIB_ROOT=/opt/csr/common
	EASY_ROOT=/usr
  DRC_ROOT=/home/ds
fi
echo "[BUILD] args: TOP_DIR=${TOP_DIR} PACKAGE=${PACKAGE} VERSION=${VERSION} RELEASE=${RELEASE} SPEC=${SPEC_FILE}"
echo "[BUILD] TBLIB_ROOT=${TBLIB_ROOT}"
echo "[BUILD] EASY_ROOT=${EASY_ROOT}"
echo "[BUILD] DRC_ROOT=${DRC_ROOT}"

echo "[BUILD] config..."
cd $TOP_DIR
chmod +x build.sh
./build.sh init
./configure  --with-test-case=no --with-release=yes --with-tblib-root=$TBLIB_ROOT --with-easy-root=$EASY_ROOT --with-drc-root=/home/ds
echo "[BUILD] make dist..."
make dist-gzip

PREFIX=/home/admin/oceanbase
TMP_DIR=/${TOP_DIR}/oceanbase-tmp.$$
echo "[BUILD] TMP_DIR=${TMP_DIR}"
echo "[BUILD] PREFIX=${PREFIX}"
echo "[BUILD] RELEASE=${RELEASE}"
echo "[BUILD] PACKAGE=${PACKAGE}"

echo "[BUILD] create tmp dirs..."
mkdir -p ${TMP_DIR}
mkdir -p ${TMP_DIR}/BUILD
mkdir -p ${TMP_DIR}/RPMS
mkdir -p ${TMP_DIR}/SOURCES
mkdir -p ${TMP_DIR}/SRPMS
cp oceanbase-${VERSION}.tar.gz ${TMP_DIR}/SOURCES
cd ${TMP_DIR}/BUILD
tar xfz ${TMP_DIR}/SOURCES/oceanbase-${VERSION}.tar.gz oceanbase-${VERSION}/rpm/${SPEC_FILE}

echo "[BUILD] make rpms..."
rpmbuild --define "_topdir ${TMP_DIR}" --define "REDHAT ${REDHAT}" --define "NAME ${PACKAGE}" --define "VERSION ${VERSION}" --define "_prefix ${PREFIX}" --define "RELEASE ${RELEASE}" -ba ${TMP_DIR}/BUILD/oceanbase-${VERSION}/rpm/${SPEC_FILE}
echo "[BUILD] make rpms done"

cd ${TOP_DIR}
find ${TMP_DIR}/RPMS/ -name "*.rpm" -exec mv '{}' . \;
rm -rf ${TMP_DIR}
mv *.rpm rpm/
