DIRS="t t2"
for dir in $DIRS
do
  rdir=`echo $dir | sed "s/t/r/"`
  for t in `ls $dir/*.test`
  do
    r=`echo $t | cut -d "/" -f 2`
    r="$rdir/$r.result"
    ./verify.sh etc/pgsql.properties $t $r
  done
done
