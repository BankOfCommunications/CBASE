/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_privilege_type.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_PRIVILEGE_TYPE_H
#define _OB_PRIVILEGE_TYPE_H 1

#ifdef __cplusplus
extern "C" {
#endif

  typedef enum ObPrivilegeType
  {
    OB_PRIV_INVALID = 0,      /*invalid privilege type*/
    OB_PRIV_ALL = 1,
    OB_PRIV_ALTER = 2,
    OB_PRIV_CREATE = 3,
    OB_PRIV_CREATE_USER = 4,
    OB_PRIV_DELETE = 5,
    OB_PRIV_DROP = 6,
    OB_PRIV_GRANT_OPTION = 7,
    OB_PRIV_INSERT = 8,
    OB_PRIV_UPDATE = 9,
    OB_PRIV_SELECT = 10,
    OB_PRIV_REPLACE = 11,
    OB_PRIV_NUM
  } ObPrivilegeType;
  const char* ob_priv_type_str(const ObPrivilegeType priv_type);

#ifdef __cplusplus
}
#endif

#endif /* _OB_PRIVILEGE_TYPE_H */
