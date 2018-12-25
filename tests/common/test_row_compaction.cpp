#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include "ob_malloc.h"
#include "page_arena.h"
#include "ob_scanner.h"
#include "ob_action_flag.h"
#include "ob_row_compaction.h"
#include "utility.h"
#include "test_rowkey_helper.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace common;

ObRowCompaction *rc = NULL;;

static CharArena allocator_;

class MockIterator : public ObIterator
{
  struct CellNode
  {
    ObCellInfo cell_info;
    bool is_row_changed;
    CellNode *next;
  };
  public:
    MockIterator() : head_(NULL), tail_(NULL), iter_(NULL)
    {
    };
    ~MockIterator()
    {
    };
  public:
    int next_cell()
    {
      int ret = OB_SUCCESS;
      if (NULL == iter_)
      {
        if (NULL == (iter_ = head_))
        {
          ret = OB_ITER_END;
        }
      }
      else if (NULL == iter_->next)
      {
        ret = OB_ITER_END;
      }
      else
      {
        iter_ = iter_->next;
      }
      return ret;
    };
    int get_cell(ObCellInfo **cell_info)
    {
      int ret = OB_SUCCESS;
      if (NULL == iter_)
      {
        ret = OB_ITER_END;
      }
      else
      {
        *cell_info = &(iter_->cell_info);
      }
      return ret;
    };
    int get_cell(ObCellInfo **cell_info, bool *is_row_changed)
    {
      int ret = OB_SUCCESS;
      if (NULL == iter_)
      {
        ret = OB_ITER_END;
      }
      else
      {
        *cell_info = &(iter_->cell_info);
        *is_row_changed = iter_->is_row_changed;
      }
      return ret;
    };
    void reset_iter()
    {
      iter_ = NULL;
    };
  public:
    void add_cell(ObCellInfo cell_info, bool is_row_changed)
    {
      CellNode *node = (CellNode*)allocator_.alloc(sizeof(CellNode));
      if (NULL != node)
      {
        node->cell_info = cell_info;
        node->is_row_changed = is_row_changed;
        node->next = NULL;
        if (NULL == tail_)
        {
          head_ = node;
        }
        else
        {
          tail_->next = node;
        }
        tail_ = node;
      }
    };
  private:
    PageArena<char*> allocator_;
    CellNode *head_;
    CellNode *tail_;
    CellNode *iter_;
};

TEST(TestObRowCompaction, single_row_single_column)
{
  MockIterator mock;
  ObCellInfo ci;
  int64_t num = 0;

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row1", &allocator_);
  ci.column_id_ = 2;
  ci.value_.set_int(1000);
  mock.add_cell(ci, true);

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row1", &allocator_);
  ci.column_id_ = 2;
  ci.value_.set_int(1000, true);
  mock.add_cell(ci, false);

  rc->set_iterator(&mock);

  while (OB_SUCCESS == rc->next_cell())
  {
    ObCellInfo *tmp_ci = NULL;
    bool tmp_irc = false;
    bool irf_check = false;
    if (OB_SUCCESS == rc->get_cell(&tmp_ci, &tmp_irc))
    {
      num += 1;
      EXPECT_EQ(tmp_ci->table_id_, (uint64_t)1001);
      EXPECT_EQ(0 , tmp_ci->row_key_.compare(make_rowkey("row1", &allocator_)));
      EXPECT_EQ(tmp_ci->column_id_, (uint64_t)2);
      int64_t tmp_value;
      tmp_ci->value_.get_int(tmp_value);
      EXPECT_EQ(tmp_value, 2000);
      irf_check = true;
    }
    bool is_row_finished = false;
    EXPECT_EQ(OB_SUCCESS, rc->is_row_finished(&is_row_finished));
    EXPECT_EQ(irf_check, is_row_finished);
  }
  EXPECT_EQ(1, num);
}

TEST(TestObRowCompaction, single_row_multi_column)
{
  MockIterator mock;
  ObCellInfo ci;
  int64_t num = 0;

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row1", &allocator_);
  ci.column_id_ = 2;
  ci.value_.set_int(1000);
  mock.add_cell(ci, true);

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row1", &allocator_);
  ci.column_id_ = 3;
  ci.value_.set_int(2000);
  mock.add_cell(ci, false);

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row1", &allocator_);
  ci.column_id_ = 2;
  ci.value_.set_int(1000, true);
  mock.add_cell(ci, false);

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row1", &allocator_);
  ci.column_id_ = 3;
  ci.value_.set_int(2000, true);
  mock.add_cell(ci, false);

  rc->set_iterator(&mock);

  while (OB_SUCCESS == rc->next_cell())
  {
    ObCellInfo *tmp_ci = NULL;
    bool tmp_irc = false;
    if (OB_SUCCESS == rc->get_cell(&tmp_ci, &tmp_irc))
    {
      num += 1;
      EXPECT_EQ(tmp_ci->table_id_, (uint64_t)1001);
      EXPECT_EQ(0 , tmp_ci->row_key_.compare(make_rowkey("row1", &allocator_)));
      int64_t tmp_value;
      tmp_ci->value_.get_int(tmp_value);
      bool irf_check = false;
      if (2 == tmp_ci->column_id_)
      {
        EXPECT_EQ(tmp_value, 2000);
      }
      else if (3 == tmp_ci->column_id_)
      {
        EXPECT_EQ(tmp_value, 4000);
        irf_check = true;
      }
      else
      {
        EXPECT_FALSE(true);
      }
      bool is_row_finished = false;
      EXPECT_EQ(OB_SUCCESS, rc->is_row_finished(&is_row_finished));
      EXPECT_EQ(irf_check, is_row_finished);
    }
  }
  EXPECT_EQ(2, num);
}

TEST(TestObRowCompaction, multi_row_single_column)
{
  MockIterator mock;
  ObCellInfo ci;
  int64_t num = 0;

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row1", &allocator_);
  ci.column_id_ = 2;
  ci.value_.set_int(1000);
  mock.add_cell(ci, true);

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row1", &allocator_);
  ci.column_id_ = 2;
  ci.value_.set_int(1000, true);
  mock.add_cell(ci, false);

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row2", &allocator_);
  ci.column_id_ = 2;
  ci.value_.set_int(2000);
  mock.add_cell(ci, true);

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row2", &allocator_);
  ci.column_id_ = 2;
  ci.value_.set_int(2000, true);
  mock.add_cell(ci, false);

  rc->set_iterator(&mock);

  while (OB_SUCCESS == rc->next_cell())
  {
    ObCellInfo *tmp_ci = NULL;
    bool tmp_irc = false;
    if (OB_SUCCESS == rc->get_cell(&tmp_ci, &tmp_irc))
    {
      num += 1;
      EXPECT_EQ(tmp_ci->table_id_, (uint64_t)1001);
      EXPECT_EQ((uint64_t)2, tmp_ci->column_id_);
      int64_t tmp_value;
      tmp_ci->value_.get_int(tmp_value);
      bool irf_check = false;
      if (0 == tmp_ci->row_key_.compare(make_rowkey("row1", &allocator_)))
      {
        EXPECT_EQ(tmp_value, 2000);
        irf_check = true;
      }
      else if (0 == tmp_ci->row_key_.compare(make_rowkey("row2", &allocator_)))
      {
        EXPECT_EQ(tmp_value, 4000);
        irf_check = true;
      }
      else
      {
        EXPECT_FALSE(true);
      }
      bool is_row_finished = false;
      EXPECT_EQ(OB_SUCCESS, rc->is_row_finished(&is_row_finished));
      EXPECT_EQ(irf_check, is_row_finished);
    }
  }
  EXPECT_EQ(2, num);
}

TEST(TestObRowCompaction, multi_row_multi_column)
{
  MockIterator mock;
  ObCellInfo ci;
  int64_t num = 0;

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row1", &allocator_);
  ci.column_id_ = 2;
  ci.value_.set_int(1000);
  mock.add_cell(ci, true);

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row1", &allocator_);
  ci.column_id_ = 3;
  ci.value_.set_int(2000);
  mock.add_cell(ci, false);

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row1", &allocator_);
  ci.column_id_ = 2;
  ci.value_.set_int(1000, true);
  mock.add_cell(ci, false);

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row1", &allocator_);
  ci.column_id_ = 3;
  ci.value_.set_int(2000, true);
  mock.add_cell(ci, false);

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row2", &allocator_);
  ci.column_id_ = 2;
  ci.value_.set_int(3000);
  mock.add_cell(ci, true);

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row2", &allocator_);
  ci.column_id_ = 3;
  ci.value_.set_int(4000);
  mock.add_cell(ci, false);

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row2", &allocator_);
  ci.column_id_ = 2;
  ci.value_.set_int(3000, true);
  mock.add_cell(ci, false);

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row2", &allocator_);
  ci.column_id_ = 3;
  ci.value_.set_int(4000, true);
  mock.add_cell(ci, false);


  rc->set_iterator(&mock);

  while (OB_SUCCESS == rc->next_cell())
  {
    ObCellInfo *tmp_ci = NULL;
    bool tmp_irc = false;
    if (OB_SUCCESS == rc->get_cell(&tmp_ci, &tmp_irc))
    {
      num += 1;
      EXPECT_EQ(tmp_ci->table_id_, (uint64_t)1001);
      int64_t tmp_value;
      tmp_ci->value_.get_int(tmp_value);
      bool irf_check = false;
      if (0 == tmp_ci->row_key_.compare(make_rowkey("row1", &allocator_)))
      {
        if (2 == tmp_ci->column_id_)
        {
          EXPECT_EQ(tmp_value, 2000);
        }
        else if (3 == tmp_ci->column_id_)
        {
          EXPECT_EQ(tmp_value, 4000);
          irf_check = true;
        }
        else
        {
          EXPECT_FALSE(true);
        }
      }
      else if (0 == tmp_ci->row_key_.compare(make_rowkey("row2", &allocator_)))
      {
        if (2 == tmp_ci->column_id_)
        {
          EXPECT_EQ(tmp_value, 6000);
        }
        else if (3 == tmp_ci->column_id_)
        {
          EXPECT_EQ(tmp_value, 8000);
          irf_check = true;
        }
        else
        {
          EXPECT_FALSE(true);
        }
      }
      else
      {
        EXPECT_FALSE(true);
      }
      bool is_row_finished = false;
      EXPECT_EQ(OB_SUCCESS, rc->is_row_finished(&is_row_finished));
      EXPECT_EQ(irf_check, is_row_finished);
    }
  }
  EXPECT_EQ(4, num);
}

TEST(TestObRowCompaction, multi_row_multi_column_with_ext)
{
  MockIterator mock;
  ObCellInfo ci;
  int64_t num = 0;

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row1", &allocator_);
  ci.column_id_ = OB_INVALID_ID;
  ci.value_.set_ext(ObActionFlag::OP_DEL_ROW);
  mock.add_cell(ci, true);

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row1", &allocator_);
  ci.column_id_ = 2;
  ci.value_.set_int(1000);
  mock.add_cell(ci, false);

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row1", &allocator_);
  ci.column_id_ = OB_INVALID_ID;
  ci.value_.set_ext(ObActionFlag::OP_NOP);
  mock.add_cell(ci, false);

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row1", &allocator_);
  ci.column_id_ = 3;
  ci.value_.set_int(2000);
  mock.add_cell(ci, false);

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row1", &allocator_);
  ci.column_id_ = OB_INVALID_ID;
  ci.value_.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
  mock.add_cell(ci, false);

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row1", &allocator_);
  ci.column_id_ = 2;
  ci.value_.set_int(1000, true);
  mock.add_cell(ci, false);

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row1", &allocator_);
  ci.column_id_ = 3;
  ci.value_.set_int(2000, true);
  mock.add_cell(ci, false);

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row2", &allocator_);
  ci.column_id_ = OB_INVALID_ID;
  ci.value_.set_ext(ObActionFlag::OP_DEL_ROW);
  mock.add_cell(ci, true);

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row2", &allocator_);
  ci.column_id_ = 2;
  ci.value_.set_int(3000);
  mock.add_cell(ci, false);

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row2", &allocator_);
  ci.column_id_ = OB_INVALID_ID;
  ci.value_.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
  mock.add_cell(ci, false);

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row2", &allocator_);
  ci.column_id_ = 3;
  ci.value_.set_int(4000);
  mock.add_cell(ci, false);

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row2", &allocator_);
  ci.column_id_ = OB_INVALID_ID;
  ci.value_.set_ext(ObActionFlag::OP_NOP);
  mock.add_cell(ci, false);

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row2", &allocator_);
  ci.column_id_ = 2;
  ci.value_.set_int(3000, true);
  mock.add_cell(ci, false);

  ci.table_id_ = 1001;
  ci.row_key_ = make_rowkey("row2", &allocator_);
  ci.column_id_ = 3;
  ci.value_.set_int(4000, true);
  mock.add_cell(ci, false);


  rc->set_iterator(&mock);

  while (OB_SUCCESS == rc->next_cell())
  {
    ObCellInfo *tmp_ci = NULL;
    bool tmp_irc = false;
    if (OB_SUCCESS == rc->get_cell(&tmp_ci, &tmp_irc))
    {
      fprintf(stderr, "%s\n", print_cellinfo(tmp_ci));
      num += 1;
      EXPECT_EQ(tmp_ci->table_id_, (uint64_t)1001);
      int64_t tmp_value;
      tmp_ci->value_.get_int(tmp_value);
      bool irf_check = false;
      if (0 == tmp_ci->row_key_.compare(make_rowkey("row1", &allocator_)))
      {
        if (2 == tmp_ci->column_id_)
        {
          EXPECT_EQ(tmp_value, 2000);
        }
        else if (3 == tmp_ci->column_id_)
        {
          EXPECT_EQ(tmp_value, 4000);
          irf_check = true;
        }
        else if (OB_INVALID_ID == tmp_ci->column_id_)
        {
          //EXPECT_EQ(true, ObActionFlag::OP_NOP == tmp_ci->value_.get_ext()
          //                || ObActionFlag::OP_DEL_ROW == tmp_ci->value_.get_ext()
          //                || ObActionFlag::OP_ROW_DOES_NOT_EXIST == tmp_ci->value_.get_ext());
          EXPECT_EQ(true, ObActionFlag::OP_DEL_ROW == tmp_ci->value_.get_ext());
        }
        else
        {
          EXPECT_FALSE(true);
        }
      }
      else if (0 == tmp_ci->row_key_.compare(make_rowkey("row2", &allocator_)))
      {
        if (2 == tmp_ci->column_id_)
        {
          EXPECT_EQ(tmp_value, 6000);
        }
        else if (3 == tmp_ci->column_id_)
        {
          EXPECT_EQ(tmp_value, 8000);
          irf_check = true;
        }
        else if (OB_INVALID_ID == tmp_ci->column_id_)
        {
          //EXPECT_EQ(true, ObActionFlag::OP_NOP == tmp_ci->value_.get_ext()
          //                || ObActionFlag::OP_DEL_ROW == tmp_ci->value_.get_ext()
          //                || ObActionFlag::OP_ROW_DOES_NOT_EXIST == tmp_ci->value_.get_ext());
          EXPECT_EQ(true, ObActionFlag::OP_DEL_ROW == tmp_ci->value_.get_ext());
        }
        else
        {
          EXPECT_FALSE(true);
        }
      }
      else
      {
        EXPECT_FALSE(true);
      }
      bool is_row_finished = false;
      EXPECT_EQ(OB_SUCCESS, rc->is_row_finished(&is_row_finished));
      EXPECT_EQ(irf_check, is_row_finished);
    }
  }
  EXPECT_EQ(6, num);
}

void build_iterator(MockIterator &iter, const int64_t rn, const int64_t cn)
{
  ObCellInfo ci;
  ci.table_id_ = 1001;
  static char rk_buf[1024][128];

  for (int64_t r = 0; r < rn; r++)
  {
    sprintf(rk_buf[r], "row%ld", r);
    ci.row_key_ = make_rowkey(rk_buf[r]);
    for (int64_t i = 0; i < cn; i++)
    {
      ci.column_id_ = i % 16;
      ci.value_.set_int(i + 2, true);
      iter.add_cell(ci, 0 == i);
    }
  }
}

void build_scanner(ObIterator &iter, ObScanner &scanner, char *buffer, const int64_t buf_len, int64_t &pos)
{
  ObCellInfo *ci = NULL;
  while(OB_SUCCESS == iter.next_cell())
  {
    if (OB_SUCCESS == iter.get_cell(&ci)
        && NULL != ci)
    {
      scanner.add_cell(*ci);
    }
  }
  scanner.serialize(buffer, buf_len, pos);
}

void multi_update_perf(const int64_t times, const int64_t rn, const int64_t cn)
{
  ObRowCompaction rc;
  MockIterator iter;
  ObScanner scanner;
  const int64_t buf_len = 2 * 1024 * 1024;
  char *buffer = new char[buf_len];
  int64_t pos = 0;
  build_iterator(iter, rn, cn);

  int64_t timeu = tbsys::CTimeUtil::getTime();
  for (int64_t i = 0; i < times; i++)
  {
    scanner.reset();
    iter.reset_iter();
    rc.set_iterator(&iter);
    pos = 0;
    build_scanner(rc, scanner, buffer, buf_len, pos);
  }
  fprintf(stderr, "row_compaction times=%ld row=%ld cell=%ld/row timeu=%ld size=%ld\n",
          times, rn, cn, tbsys::CTimeUtil::getTime() - timeu, pos);
  FILE *fd = fopen("./rc.data", "w");
  fwrite(buffer, 1, pos, fd);
  fclose(fd);

  timeu = tbsys::CTimeUtil::getTime();
  for (int64_t i = 0; i < times; i++)
  {
    scanner.reset();
    iter.reset_iter();
    pos = 0;
    build_scanner(iter, scanner, buffer, buf_len, pos);
  }
  fprintf(stderr, "no_compaction times=%ld row=%ld cell=%ld/row timeu=%ld size=%ld\n",
          times, rn, cn, tbsys::CTimeUtil::getTime() - timeu, pos);
  fd = fopen("./nrc.data", "w");
  fwrite(buffer, 1, pos, fd);
  fclose(fd);

  delete buffer;
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  rc = new ObRowCompaction();
  testing::InitGoogleTest(&argc,argv); 
  int ret = RUN_ALL_TESTS();
  //multi_update_perf(100, 1000, 112);
  //multi_update_perf(100, 1000, 32);
  delete rc;
  return ret;
}

