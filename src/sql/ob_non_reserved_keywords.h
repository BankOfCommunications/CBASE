/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_non_reserved_keywords.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */

#ifndef OB_NON_RESERVED_KEYWORDS_H_
#define OB_NON_RESERVED_KEYWORDS_H_

typedef struct _NonReservedKeyword
{
  const char *keyword_name;
  int keyword_type;
} NonReservedKeyword;

extern const NonReservedKeyword *non_reserved_keyword_lookup(const char *word);

#endif /* OB_NON_RESERVED_KEYWORDS_H_ */

