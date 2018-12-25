/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sql_atomic.h is for what ...
 *
 * Version: ***: ob_sql_atomic.h  Thu Jan 10 10:47:13 2013 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want
 *
 */
#ifndef OB_SQL_ATOMIC_H_
#define OB_SQL_ATOMIC_H_

#include "common/ob_atomic.h"

//如果pv 大于0      pv--   返回新pv的值
//如果pv 小于等于0  pv不变 返回pv-1的值
int64_t atomic_dec_positive(volatile uint32_t *pv);

#endif
