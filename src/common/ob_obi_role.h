/*
 * Copyright (C) 2007-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * The role of an oceanbase instance. 
 *
 * Version: $Id$
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *     - init
 */
#ifndef _OB_OBI_ROLE_H
#define _OB_OBI_ROLE_H 1

#include "common/ob_define.h"
#include "ob_atomic.h"
#include "tblog.h"

namespace oceanbase
{
namespace common
{
class ObiRole
{
public:
  enum Role
  {
    MASTER = 0, 
    SLAVE = 1,
    INIT = 2
  };
public:
  ObiRole();
  virtual ~ObiRole();
  Role get_role() const;
  void set_role(const Role& role);

  const char* get_role_str() const;
  bool operator== (const ObiRole& role) const;
  NEED_SERIALIZE_AND_DESERIALIZE;
private:
  Role role_;
};

inline ObiRole::Role ObiRole::get_role() const
{
  return role_;
}

inline void ObiRole::set_role(const Role& role)
{
  atomic_exchange(reinterpret_cast<uint32_t*>(&role_), role);
  TBSYS_LOG(DEBUG, "set obi role, role=%d", role_);
}

inline bool ObiRole::operator== (const ObiRole& other) const
{
  return this->role_ == other.role_;
}

inline const char* ObiRole::get_role_str() const
{
  switch (role_)
  {
    case MASTER: return "MASTER";
    case SLAVE:  return "SLAVE";
    case INIT:   return "INIT";
    default:     return "UNKNOWN";
  }
}

} // end namespace rootserver
} // end namespace oceanbase

#endif /* _OB_OBI_ROLE_H */

