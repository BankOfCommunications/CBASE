/*
 * =====================================================================================
 *
 *       Filename:  db_msg_report.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/12/2011 02:39:33 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh (), yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */

#ifndef  DB_MSG_REPORT_INC
#define  DB_MSG_REPORT_INC

enum MsgType {
  MSG_INFO,
  MSG_WARN,
  MSG_ERROR,
  MSG_MAX
};

void report_msg(MsgType type, const char *msg, ...);

#endif   /* ----- #ifndef db_msg_report_INC  ----- */
