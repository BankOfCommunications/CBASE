/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_easy_log.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_EASY_LOG_H
#define _OB_EASY_LOG_H 1

// customize libeasy log format function
void ob_easy_log_format(int level, const char *file, int line, const char *function, const char *fmt, ...);

#endif /* _OB_EASY_LOG_H */
