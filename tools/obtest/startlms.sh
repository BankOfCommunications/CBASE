if [ -f $WORKSPACE/etc/mergeserver.config.bin ]
then
        exec $WORKSPACE/bin/mergeserver -N
else
        exec $WORKSPACE/bin/mergeserver -N $ARGUMENT
fi
