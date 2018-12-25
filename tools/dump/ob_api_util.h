/*
 * =====================================================================================
 *
 *       Filename:  ob_api_util.h
 *
 *
 *        Version:  1.0
 *        Created:  04/14/2011 04:47:54 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh (DBA Group), yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */

#ifndef  OB_API_UTIL_INC
#define  OB_API_UTIL_INC

#include "common/ob_server.h"
#include "common/ob_string.h"
#include "common/ob_scanner.h"
#include "common/ob_result.h"
#include "common/utility.h"

void dump_obstring(const oceanbase::common::ObString &str,char *desc)
{
    TBSYS_LOG(INFO, "%s,%d:%d:%d:%d:%d:%d:%d:%d", desc,
              str.ptr()[0],
              str.ptr()[1],
              str.ptr()[2],
              str.ptr()[3],
              str.ptr()[4],
              str.ptr()[5],
              str.ptr()[6],
              str.ptr()[7]
              );
}

#endif   /* ----- #ifndef ob_api_util_INC  ----- */
