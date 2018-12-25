/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_mysql_define.h is for what ...
 *
 * Version: ***: ob_mysql_define.h  Sun Aug  5 15:20:27 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@taobao.com
 *     -some work detail if you want
 *
 */
#ifndef OB_MYSQL_DEFINE_H_
#define OB_MYSQL_DEFINE_H_

enum enum_server_command
{
 COM_SLEEP, COM_QUIT, COM_INIT_DB, COM_QUERY, COM_FIELD_LIST,
 COM_CREATE_DB, COM_DROP_DB, COM_REFRESH, COM_SHUTDOWN, COM_STATISTICS,
 COM_PROCESS_INFO, COM_CONNECT, COM_PROCESS_KILL, COM_DEBUG, COM_PING,
 COM_TIME, COM_DELAYED_INSERT, COM_CHANGE_USER, COM_BINLOG_DUMP, COM_TABLE_DUMP,
 COM_CONNECT_OUT, COM_REGISTER_SLAVE, COM_STMT_PREPARE, COM_STMT_EXECUTE, COM_STMT_SEND_LONG_DATA,
 COM_STMT_CLOSE, COM_STMT_RESET, COM_SET_OPTION, COM_STMT_FETCH, COM_DAEMON,
 COM_END/*30*/, COM_DELETE_SESSION    /* COM_DELETE_SESSION 不是标准的mysql包类型 这个是用来处理删除session的包
                                    在连接断开的时候，需要删除session 但是此时有可能在回调函数disconnect中获取不到
                                    session的锁，此时会往obmysql的队列中添加一个异步任务*/
};
#endif
