#export LD_LIBRARY_PATH=/usr/local/mysql/lib/mysql/
DBNAME=$1
WH=$2
HOST=$3
STEP=2

./tpcc_load $HOST $DBNAME admin admin $WH 1 1 $WH >> 1.out &

x=1

while [ $x -le $WH ]
do
 echo $x $(( $x + $STEP - 1 ))
./tpcc_load $HOST $DBNAME admin admin $WH 2 $x $(( $x + $STEP - 1 ))  >> 2_$x.out &
./tpcc_load $HOST $DBNAME admin admin $WH 3 $x $(( $x + $STEP - 1 ))  >> 3_$x.out &
./tpcc_load $HOST $DBNAME admin admin $WH 4 $x $(( $x + $STEP - 1 ))  >> 4_$x.out &
 x=$(( $x + $STEP ))
done
wait
