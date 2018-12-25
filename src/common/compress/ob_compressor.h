/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *
 * ob_compressor.h is for what ...
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef  OCEANBASE_COMMON_COMPRESS_OB_COMPRESSOR_H_
#define  OCEANBASE_COMMON_COMPRESS_OB_COMPRESSOR_H_

#include <stdint.h>
#include <stdlib.h>

#define MAX_LIB_NAME_LENGTH 256

class ObCompressor
{
  public:
    enum
    {
      COM_E_NOERROR = 0,      // 正常
      COM_E_INVALID_PARAM = 1,  // 参数错
      COM_E_NOIMPL = 2,      // 没有实现
      COM_E_OVERFLOW = 3,      // 缓冲区溢出
      COM_E_DATAERROR = 4,    // 数据格式错误
      COM_E_INTERNALERROR = 5,  // 内部错误
      // ...
    };
  private:
    const static uint64_t INTERFACE_VERSION = ((uint64_t)1)<<48;
  public:
    ObCompressor() : sohandle_(NULL)
    {
      return;
    };
    virtual ~ObCompressor()
    {
      return;
    };
  public:
  
    /*
     * 数据压缩接口，该接口只负责压缩，并不负责对压缩后的长度做检查， 
     * 所以应用层需要按照如下三种情况进行处理： 
     *   1. dst_data_size < src_data_size，压缩后比原数据长度小，
     * 使用压缩后的数据，该压缩数据在dst_buffer中。
     *   2. dst_data_size == src_data_size，压缩后与原数据长度相等，
     * dst_buffer中不包含压缩后的数据，使用未压缩的数据。
     *   3. dst_data_size > src_data_size，压缩后比原数据长度大，
     * dst_buffer中不包含压缩后的数据，使用未压缩的数据。
     *  
     * @param [in] dst_buff 存放结果数据的缓冲区
     * @param [in] dst_buff_size 传入的结果数据缓冲区的大小
     * @param [in] dst_data_size 压缩结果数据的大小
     * @param [in] src_buff 输出数据缓冲区
     * @param [in] src_data_size 输入数据大小
     */
    virtual int compress(const char *src_buffer,
                        const int64_t src_data_size,
                        char *dst_buffer,
                        const int64_t dst_buffer_size,
                        int64_t &dst_data_size) = 0;
    /*
     * 数据压缩接口，该接口只负责解压缩，并不负责对解压缩后的长度做检查， 
     * 应用层需要判断dst_data_size是否与期望的解压缩后的数据长度一致，不 
     * 一致则出错了，解压缩后的数据在dst_buffer中。
     *  
     * @param [in] dst_buff 存放结果数据的缓冲区
     * @param [in] dst_buff_size 传入的结果数据缓冲区的大小
     * @param [in] dst_data_size 压缩结果数据的大小
     * @param [in] src_buff 输出数据缓冲区
     * @param [in] src_data_size 输入数据大小
      */  
    virtual int decompress(const char *src_buffer,
                        const int64_t src_data_size,
                        char *dst_buffer,
                        const int64_t dst_buffer_size,
                        int64_t &dst_data_size) = 0;
    /*
     * 设置每个压缩块或滑动窗口大小
     * 不是所有算法都必须提供
     * 
     * @param [in] compress_block_size 要设定的压缩块或滑动窗口大小
     */   
    virtual int set_compress_block_size(const int64_t compress_block_size)
    {
      (void)(compress_block_size);
      return COM_E_NOIMPL;
    };
  
    /*
     * 设置数据压缩级别
     * 不是所有算法都必须提供
     *
     * @param [in] compress_level  要设定的压缩等级
     */
    virtual int set_compress_level(const int64_t compress_level)
    {
      (void)(compress_level);
      return COM_E_NOIMPL;
    };
  
    /*
     * 根据传入的大小计算压缩后最大的可能的溢出大小
     * 不是所有算法都必须提供
     */
    virtual int64_t get_max_overflow_size(const int64_t src_data_size) const
    {
      (void)(src_data_size);
      return (int64_t)-1;
    };
  
    /*
     * 获取当前压缩算法名
     */
    virtual const char *get_compressor_name() const = 0;
  
    /*
     * 获取接口版本号
     * 子类不需要实现
     */
    int64_t get_interface_ver() const
    {
      return INTERFACE_VERSION;
    };

  private:
    friend ObCompressor *create_compressor(const char *compressor_lib_name);
    friend void destroy_compressor(ObCompressor *compressor);
    void set_sohandle(void *sohandle)
    {
      sohandle_ = sohandle;

    };
    void *get_sohandle()
    {
      return sohandle_;
    };
  private:
    void *sohandle_;
};

/*
 * 指向动态库要实现的函数的指针声明
 */
typedef ObCompressor* (*compressor_constructor_t)();
typedef void (*compressor_deconstructor_t)(ObCompressor *compressor);

/*
 * 根据传入的名字加载动态库， 返回一个压缩方法实例
 */
extern ObCompressor *create_compressor(const char *compressor_lib_name);

/* 
 * 销毁一个压缩方法实例
 */
extern void destroy_compressor(ObCompressor *compressor);

#endif
