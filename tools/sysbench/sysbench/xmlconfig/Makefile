JAVA=`which java`

TOPDIR=../
SIMONTOOL_JAR=${TOPDIR}/libs/SimonTool.jar

SIMONTOOL = ${JAVA}  -jar $(SIMONTOOL_JAR)
	
sysbench_OBJ = simon_sysbench.o

all:libsimon_sysbench.a

.cc.o:
	g++ -c $^ -g -O0 -fPIC  -o $@ -I../clientcommon
.cpp.o:
	g++ -c $^ -g -O0 -fPIC  -o $@ -I../clientcommon


libsimon_sysbench.a:${sysbench_OBJ}
	ar -cru $@ $^


simon_sysbench.cc:simon_sysbench.xml
	${SIMONTOOL} $^ simon_sysbench

.PHONY:clean
clean:
	rm *.o -rf
	rm *.a -rf
	rm simon_sysbench.cc -rf
	rm simon_sysbench.h -rf
#	rm *.so

install:

