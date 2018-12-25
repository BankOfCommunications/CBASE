echo $1
echo $2
echo $3
java -cp frame.jar:log4j-1.2.15.jar:slf4j-log4j12-1.4.3.jar:commons-logging-1.1.1.jar:slf4j-api-1.4.3.jar:postgresql-9.1-901.jdbc3.jar  com.etao.obtest.connector.Test $1 $2 $3
