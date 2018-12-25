all: ob_sql_example
CC=g++
LDFLAGS = -L/usr/lib64/mysql/ -lmysqlclient
ob_sql_example: ob_sql_example.o
	$(CC) -o $@ $^ $(LDFLAGS)
clean:
	$(RM) *.o ob_sql_example
