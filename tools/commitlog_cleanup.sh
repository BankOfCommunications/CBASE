#!/bin/sh

#########################################
# This script cleanup the commit log directory,
# following 2 steps to do cleaning:
#   1. move the unused commit log and checkpoint file to TRASH_DIR directory;
#   2. remove the files in TRASH_DIR directory older than LOG_DURATION_DAYS.
#########################################

############### Parameter ################

# directory of commit log
COMMIT_LOG_DIR=commitlog

# the duration log reserved
LOG_DURATION_DAYS=15

# checkpoint extension
CHECKPOINT_EXT="checkpoint"
CHECKPOINT_EXT_LIST="$CHECKPOINT_EXT rtable clist"

TRASH_DIR=$COMMIT_LOG_DIR/trash
UPS_REPLAY_POINT_FN=$COMMIT_LOG_DIR/log_replay_point

############## Function ################
log()
{
  echo "[`date +%F\ %T`] $1" > /dev/stderr
}

is_all_digits()
{
  if [ -z $1 ]; then
    return 1
  else
    tt=`echo $1 | sed -n "/^[0-9]\+$/p"`
    if [ "x$tt" == "x" ]; then
      return 1
    else
      return 0
    fi 
  fi
}

get_rs_del_point()
{
  del_point_t=1
  for file in `ls $COMMIT_LOG_DIR`; do
    id=`echo $file | sed -n "s|\(\d*\).$CHECKPOINT_EXT$|\1|p"`
    if is_all_digits $id; then
      if [ $id -gt $del_point_t ]; then
        del_point_t=$id
      fi
    fi
  done
  echo $del_point_t
}

get_del_point()
{
  if [ -r $UPS_REPLAY_POINT_FN ]; then
    cat $UPS_REPLAY_POINT_FN
  else
    get_rs_del_point
  fi
}

init()
{
# create trash directory
  if [ ! -d $TRASH_DIR ]; then
    mkdir -p $TRASH_DIR
    log "INFO  make trash directory $TRASH_DIR"
  fi

# get the delete point
  DEL_POINT=`get_del_point`
  if ! is_all_digits $DEL_POINT; then
    log "DEL_POINT $DEL_POINT is not all digits"
    exit 1
  fi
}

trash_log()
{
  for log in `ls $COMMIT_LOG_DIR`; do
    if is_all_digits $log; then
      if [ $log -lt $DEL_POINT ]; then
        if ! mv $COMMIT_LOG_DIR/$log $TRASH_DIR; then
          ret=$?
          log "ERROR  Command mv $COMMIT_LOG_DIR/$log $TRASH_DIR error: $ret"
          exit $ret
        else
          log "INFO  move $COMMIT_LOG_DIR/$log to trash"
        fi
      fi
    fi
  done
}

trash_checkpoint()
{
  for ckpt in `ls $COMMIT_LOG_DIR`; do
    for ckpt_ext in $CHECKPOINT_EXT_LIST; do
      id=`echo $ckpt | sed -n "s|\(\d*\).$ckpt_ext$|\1|p"`
      if is_all_digits $id; then
        break
      fi
    done
    if is_all_digits $id; then
      if [ $id -lt $DEL_POINT ]; then
        if ! mv $COMMIT_LOG_DIR/$ckpt $TRASH_DIR; then
          ret=$?
          log "ERROR  Command mv $COMMIT_LOG_DIR/$ckpt $TRASH_DIR error: $ret"
          exit $ret
        else
          log "INFO  move $COMMIT_LOG_DIR/$ckpt to trash"
        fi
      fi
    fi
  done
}

remove_trash()
{
  if ! find $TRASH_DIR -name "*" -type f -mtime +$LOG_DURATION_DAYS -exec rm {} \;; then
    ret=$?
    log "ERROR  Command find $TRASH_DIR -name \"*\" -type f -mtime +$LOG_DURATION_DAYS -exec rm {} \; error: $ret"
    exit $ret
  fi
}

############### Main ################

init
remove_trash
trash_log
trash_checkpoint

