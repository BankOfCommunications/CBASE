/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: sqltest_pattern.cpp,v 0.1 2012/02/27 11:28:13 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include "sqltest_pattern.h"

SqlTestPattern::SqlTestPattern()
{
  memset(file_, 0x00, sizeof(file_));
  fd_ = NULL;
  num_patterns_ = 0;
  patterns_ = NULL;
}

SqlTestPattern::~SqlTestPattern()
{
  if (NULL != patterns_)
  {
    for (int i = 0; i < num_patterns_; ++i)
    {
      delete[] patterns_[i];
    }
    delete[] patterns_;
  }

  if (NULL != fd_)
  {
    fclose(fd_);
    fd_ = NULL;
  }
}

int SqlTestPattern::init(char* pattern_file)
{
  int err = 0;
  assert(NULL != pattern_file);
  assert(NULL == patterns_);
  strcpy(file_, pattern_file);
  fd_ = fopen(file_, "r");
  if (fd_ == NULL)
  {
    TBSYS_LOG(WARN, "failed to open file, pattern_file=%s", pattern_file);
  }
  else
  {
    err = read_patterns();
    if (0 != err)
    {
      TBSYS_LOG(WARN, "failed to read patterns, file=%s, err=%d", pattern_file, err);
    }
  }

  return err;
}

int SqlTestPattern::get_next_pattern(char* pattern)
{
  int err = 0;

  assert(NULL != pattern);
  assert(num_patterns_ > 0 && NULL != patterns_);

  int rand_val = rand() % num_patterns_;
  strcpy(pattern, patterns_[rand_val]);

  return err;
}

int SqlTestPattern::read_patterns()
{
  int err = 0;

  char line[MAX_LINE_NUM];
  patterns_ = new char*[MAX_PATTERN_NUM];
  int line_idx = 0;

  while (fgets(line, sizeof(line), fd_) != NULL)
  {
    patterns_[line_idx] = new char[MAX_LINE_NUM];
    if (strstr(line, "#") != line && !is_empty(line))
    {
      strcpy(patterns_[line_idx], line);
      ++line_idx;
    }
  }

  num_patterns_ = line_idx;

  return err;
}

bool SqlTestPattern::is_empty(const char* line)
{
  assert(line != NULL);
  bool empty = true;
  for (int i = 0; line[i] != '\0'; ++i)
  {
    if (line[i] != ' ')
    {
      empty = false;
      break;
    }
  }

  return empty;
}

int SqlTestPattern::dump()
{
  assert(patterns_ != NULL);
  fprintf(stderr, "num_patterns=%d\n", num_patterns_);
  for (int i = 0; i < num_patterns_; ++i)
  {
    fprintf(stderr, "pattern[%d]: %s\n", i, patterns_[i]);
  }

  return 0;
}


