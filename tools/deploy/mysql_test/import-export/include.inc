#LOCAL_DIR is the deploy directory, E.G. ~/deploy
--let    $IE=$LOCAL_DIR/mysql_test/import-export
--let $TOOLS=$LOCAL_DIR/tools
--let   $TMP=$IE/tmp
--let  $CONF=$IE/conf

# ob_import & ob_import_v2 share the same directory of data
--let  $IM_DEL=$IE/im-del
--let  $EX_DEL=$TMP/ex-del

--let $IM1_LOG=$TMP/im1-log
--let $IM2_LOG=$TMP/im2-log
--let  $EX_LOG=$TMP/ex-log

--let $IM1_BAD=$TMP/im1-bad
--let $IM2_BAD=$TMP/im2-bad
--let  $EX_BAD=$TMP/ex-bad

--let $IM1_EXE=$TOOLS/ob_import
--let $IM2_EXE=$TOOLS/ob_import_v2
--let  $EX_EXE=$TOOLS/ob_export
--let $UPS_ADMIN_EXE=$TOOLS/ups_admin
