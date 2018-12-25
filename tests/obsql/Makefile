OUT_DIR=./out
SRC_DIR=.

#complie option
CPP=g++

CFLAGS=-Wall -g

LIBFLAGS=-L/usr/lib64/ -L/usr/local/lib 
LIBS= -lmysqlclient_r -lgtest -lpthread 


#obj=mysql_err
obj=mysql_client mysql_err mysql_init mysql_lib_init mysql_list mysql_ping mysql_query mysql_query_ps mysql_server mysql_thread  mysql_trx


.PHONY: clear all  tear

all: mkdir $(obj) move
	   @echo "make finished !"

mkdir:
	   @rm -rf out
	   @mkdir out

$(obj): % : %.c
	   @${CPP} -o $@  -lgtest ${CFLAGS} $< ${LIBFLAGS} ${LIBS}


clear:
	   @rm  -rf *.o
		 @rm  -rf ./out

tear:
	   @rm  -rf *.o

move:
	   @mv ${SRC_DIR}/$(obj)  ${OUT_DIR}
