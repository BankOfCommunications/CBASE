/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *     - some work details if you want
 */

#ifndef OCEANBASE_UPDATESERVER_OB_SLAVE_STAT_H_
#define OCEANBASE_UPDATESERVER_OB_SLAVE_STAT_H_


namespace oceanbase
{
  namespace updateserver
  {
    enum ObiSlaveStat
    {
      MIN_SLAVE_STAT = 0,
      FOLLOWED_SLAVE = 1,
      STANDALONE_SLAVE = 2,
      UNKNOWN_SLAVE = 3,
      MAX_SLAVE_STAT = 4,
    };
  } // end namespace updateserver
} // end namespace oceanbase

#endif // OCEANBASE_UPDATESERVER_OB_SLAVE_STAT_H_
