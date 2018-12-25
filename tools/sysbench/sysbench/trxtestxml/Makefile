JAVA=`which java`

TOPDIR=../
SIMONTOOL_JAR=${TOPDIR}/libs/SimonTool.jar

SIMONTOOL = ${JAVA}  -jar $(SIMONTOOL_JAR)
	
trxtest_OBJ = simon_trxtest.o

all:libsimon_trxtest.a

.cc.o:
	g++ -c $^ -g -O0 -fPIC  -o $@ -I../clientcommon
.cpp.o:
	g++ -c $^ -g -O0 -fPIC  -o $@ -I../clientcommon


libsimon_trxtest.a:${trxtest_OBJ}
	ar -cru $@ $^


simon_trxtest.cc:simon_trxtest.xml
	${SIMONTOOL} $^ simon_trxtest

.PHONY:clean
clean:
	rm *.o -rf
	rm *.a -rf
	rm simon_trxtest.cc -rf
	rm simon_trxtest.h -rf
#	rm *.so

install:

