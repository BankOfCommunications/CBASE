#include "task_output_file.h"
#include "task_utils.h"
#include "common/ob_define.h"
#include "common/ob_result.h"
#include "common/ob_malloc.h"
#include "common/data_buffer.h"
#include "common/utility.h"

namespace oceanbase
{
  namespace tools
  {
    using namespace oceanbase::common;

    TaskOutputFile::TaskOutputFile()
      : file_name_(NULL), env_(NULL), file_(NULL), split_rowkey_(false), conf_(NULL), delima_(1), rec_delima_(10) { }

    TaskOutputFile::TaskOutputFile(const char *file_name, Env *env)
      : file_name_(file_name),env_(env), file_(NULL),
      split_rowkey_(false), conf_(NULL), delima_(1), rec_delima_(10) { }

    TaskOutputFile::~TaskOutputFile()
    {
      close();
    }


    int TaskOutputFile::handle_rk_null(common::ObDataBuffer &buffer, ObCellInfo *cell,
        TableConf::ColumnIterator &col_itr, bool add_delima) const
    {
      UNUSED(cell);
      int ret = OB_SUCCESS;
      while (col_itr != conf_->column_end())// && cell->column_name_.compare(*col_itr))
      {
        if (!conf_->is_rowkey(*col_itr) && !conf_->is_null_column(*(col_itr)))
        {
          break;
        }

        if (add_delima)
        {
          ret = append_delima(buffer, delima_);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "apped delima failed");
            break;
          }
        }

        if (conf_->is_rowkey(*col_itr))
        {
          // TODO
          //ret = conf_->append_rowkey_item(cell->row_key_, *col_itr, buffer);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "can't encode rowkey");
            break;
          }
        }
#if DUMP_DEBUG
        else if (conf_->is_null_column(*col_itr))
        {
          buffer.get_data()[buffer.get_position()++] = ' ';
        }
#endif
        if (!add_delima && col_itr + 1 != conf_->column_end()) /* add delima at tail */
        {
          ret = append_delima(buffer, delima_);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "apped delima failed");
            break;
          }
        }

        col_itr++;
      }

      return ret;
    }

    int TaskOutputFile::set_table_conf(const TableConf *conf)
    {
      int ret = OB_SUCCESS;
      split_rowkey_ = true;
      conf_ = conf;
      if (!conf_)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "call set_table_conf with NULL");
      }
      else
      {
        delima_ = conf->delima();
        rec_delima_ = conf->rec_delima();
        TBSYS_LOG(DEBUG, "Using table conf, conf string = %s", conf->DebugString().c_str());
      }

      return ret;
    }

    //open file_name_
    int TaskOutputFile::open()
    {
      int ret = OB_SUCCESS;
      //check if env is valid
      if (env_ != NULL && env_->valid())
      {
        std::string fname = file_name_;
        if (env_->new_writable_file(fname, file_) != 0)
        {
          TBSYS_LOG(ERROR, "can't new writable_file");
          ret = OB_ERROR;
        }
      }
      else                                      /* create a console output */
      {
        file_ = Env::Posix()->console();
        if (file_ == NULL)
        {
          TBSYS_LOG(ERROR, "can't create console output");
          ret = OB_ERROR;
        }
        else
        {
          TBSYS_LOG(DEBUG, "using console as output file");
        }
      }

      return ret;
    }

    int TaskOutputFile::convert_result(const ObScanner &result, ObDataBuffer &buffer) const
    {
      int ret = OB_SUCCESS;
      int len = 0;
      bool first_line = true;
      ObCellInfo *cell = NULL;
      ObScannerIterator itr = result.begin();
      TableConf::ColumnIterator col_itr;

      if (conf_ != NULL)
      {
        col_itr= conf_->column_begin();
      }
      else
      {
        TBSYS_LOG(WARN, "please specify table conf");
        return OB_ERROR;
      }

      while (itr != result.end())
      {
        bool row_changed = false;

        ret = itr.get_cell(&cell, &row_changed);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "get cell failed ret[%d]", ret);
          break;
        }

        //append end of line for the last rowkey
        if (first_line)
        {
          row_changed = false;
          first_line = false;
        }
        else
        {
          if (row_changed == true)
          {
            if (ret == OB_SUCCESS)
            {
              ret = append_end_rec(buffer, rec_delima_);
              col_itr = conf_->column_begin();
            }
          }
          else
          {
            ret = append_delima(buffer, delima_);
          }

          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "append delima or end or record failed");
            break;
          }
        }

        if (split_rowkey_ && conf_)
        {
          ret = handle_rk_null(buffer, cell, col_itr, false);
        }

        if (col_itr == conf_->column_end())
        {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "task_worker conf is diff with master conf, please check it");
          break;
        }

        if (cell == NULL || cell->column_name_.compare(*col_itr))
        {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "column name mismatch ,cell column name = %s, expected name = %s",
                    std:: string(cell->column_name_.ptr(), cell->column_name_.length()).c_str(), *col_itr);
          break;
        }

        // normal column
        if (ret == OB_SUCCESS)
        {
          if (conf_->is_revise_column(cell->column_name_))
          {
            ret = conf_->revise_column(*cell, buffer);
            if (ret != OB_SUCCESS)
            {
              TBSYS_LOG(ERROR, "revise column failed, column_id=%ld", cell->column_id_);
              break;
            }
          }
          else
          {
            len = serialize_cell(cell, buffer);
            if (len < 0)
            {
              TBSYS_LOG(ERROR, "serialize cell failed");
              ret = OB_ERROR;
              break;
            }
          }
        }

        if (col_itr != conf_->column_end())
        {
          // move to next column
          col_itr++;
        }

        if (ret == OB_SUCCESS && conf_ != NULL)
        {
          ret = handle_rk_null(buffer, cell, col_itr, true);
          if (ret != OB_SUCCESS)
          {
            break;
          }
        }

        itr++;
      }

      if (ret == OB_SUCCESS)
      {
        if (col_itr != conf_->column_end())
        {
          TBSYS_LOG(ERROR, "error happens column does not all proceeded");
        }

        if (ret == OB_SUCCESS)
        {
          ret = append_end_rec(buffer, rec_delima_);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "last row, append end rec failed");
          }
        }
      }
      return ret;
    }

    int TaskOutputFile::get_buffer(ObDataBuffer &buffer)
    {
      int ret = OB_SUCCESS;
      ret = buffer_.ensure_space(2 * OB_MAX_PACKET_LENGTH);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "no enough memory for buffer");
      }
      else
      {
        buffer.set_data(buffer_.get_buffer(), buffer_.get_buffer_size());
      }

      return ret;
    }

    //append ObScanner to output file
    int TaskOutputFile::append(common::ObScanner &scanner)
    {
      int ret = OB_SUCCESS;
      ObDataBuffer buffer;

      TBSYS_LOG(DEBUG, "in task output file append");
      if (file_ == NULL)
      {
        TBSYS_LOG(ERROR, "output file is not opened");
        ret = OB_ERROR;
      }

      if (ret == OB_SUCCESS)
      {
        ret = get_buffer(buffer);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "can't get internal buffer");
        }
      }

      if (ret == OB_SUCCESS)
      {
        ret = convert_result(scanner, buffer);
        if (ret == OB_SUCCESS)
        {
          TBSYS_LOG(DEBUG, "result buffer len = %ld", buffer.get_position());
          if (file_->append(buffer) == -1)
          {
            ret = OB_ERROR;
            TBSYS_LOG(ERROR, "can't append buffer to writable file");
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "convert_result failed");
        }
      }

      return ret;
    }

    //flush data
    int TaskOutputFile::flush()
    {
      return OB_SUCCESS;
    }

    //close file_name_
    int TaskOutputFile::close()
    {
      int ret = OB_SUCCESS;
      if (file_ != NULL)
      {
        ret = file_->close();
        delete file_;
        file_ = NULL;
      }

      return ret;
    }
  }
}

