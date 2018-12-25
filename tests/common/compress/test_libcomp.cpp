#include "ob_compressor.h"
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include "gtest/gtest.h"

TEST(TestLibcomp, create)
{
  ObCompressor *nil = NULL;
  ObCompressor *comp1 = NULL;
  ObCompressor *comp2 = NULL;
  ObCompressor *snappy1 = NULL;
  ObCompressor *snappy2 = NULL;
  EXPECT_NE(nil, (comp1 = create_compressor("lzo_1.0")));
  EXPECT_NE(nil, (comp2 = create_compressor("lzo_1.0")));
  EXPECT_NE(nil, (snappy1 = create_compressor("snappy_1.0")));
  EXPECT_NE(nil, (snappy2 = create_compressor("snappy_1.0")));
  EXPECT_NE(comp1, comp2);
  EXPECT_NE(snappy1, snappy2);
  EXPECT_EQ(nil, create_compressor("invalid"));
  destroy_compressor(comp1);
  destroy_compressor(comp2);
  destroy_compressor(snappy1);
  destroy_compressor(snappy2);
  destroy_compressor(NULL);
}

TEST(TestLibcomp, compress)
{
  char *fname = (char*)"./data/comp.data";
  struct stat st;
  FILE *fd = fopen(fname, "r");
  stat(fname, &st);
  char *src_buffer = new char[st.st_size];
  fread(src_buffer, sizeof(char), st.st_size, fd);
  fclose(fd);

  ObCompressor *comp = create_compressor("lzo_1.0");
  if (NULL != comp)
  {
    char *comp_buffer = new char[st.st_size + comp->get_max_overflow_size(st.st_size)];
    char *decomp_buffer = new char[st.st_size];
    int64_t ret_size = st.st_size + comp->get_max_overflow_size(st.st_size);

    EXPECT_EQ(ObCompressor::COM_E_OVERFLOW, comp->compress(src_buffer, st.st_size, comp_buffer, st.st_size, ret_size));

    EXPECT_EQ(ObCompressor::COM_E_NOERROR, comp->compress(src_buffer, st.st_size, comp_buffer, ret_size, ret_size));
    EXPECT_EQ(ObCompressor::COM_E_NOERROR, comp->decompress(comp_buffer, ret_size, decomp_buffer, st.st_size, ret_size));
    EXPECT_EQ(st.st_size, ret_size);
    EXPECT_EQ(0, memcmp(decomp_buffer, src_buffer, st.st_size));

    EXPECT_EQ(ObCompressor::COM_E_INVALID_PARAM, comp->compress(NULL, 1, NULL, 1, ret_size));
    EXPECT_EQ(ObCompressor::COM_E_INVALID_PARAM, comp->compress(src_buffer, 0, comp_buffer, 0, ret_size));
    EXPECT_EQ(ObCompressor::COM_E_INVALID_PARAM, comp->compress(src_buffer, -1, comp_buffer, -1, ret_size));
    EXPECT_EQ(ObCompressor::COM_E_INVALID_PARAM, comp->decompress(NULL, 1, NULL, 1, ret_size));
    EXPECT_EQ(ObCompressor::COM_E_INVALID_PARAM, comp->decompress(src_buffer, 0, comp_buffer, 0, ret_size));
    EXPECT_EQ(ObCompressor::COM_E_INVALID_PARAM, comp->decompress(src_buffer, -1, comp_buffer, -1, ret_size));

    // TODO return value of lzo compress/decompress, overflow
    //EXPECT_EQ(ObCompressor::COM_E_OVERFLOW, comp->decompress(comp_buffer, ret_size, decomp_buffer, st.st_size, ret_size));
    EXPECT_EQ(ObCompressor::COM_E_DATAERROR, comp->decompress(comp_buffer, ret_size - 1, decomp_buffer, st.st_size, ret_size));

    EXPECT_EQ(ObCompressor::COM_E_NOIMPL, comp->set_compress_block_size(st.st_size));
    EXPECT_EQ(ObCompressor::COM_E_NOIMPL, comp->set_compress_level(1));

    EXPECT_EQ(0, strcmp("lzo_1.0", comp->get_compressor_name()));

    EXPECT_EQ(((int64_t)1)<<48, comp->get_interface_ver());

    delete[] decomp_buffer;
    delete[] comp_buffer;
    destroy_compressor(comp);
  }
}

TEST(TestLibcomp, compress_snappy)
{
  char *fname = (char*)"./data/comp.data";
  struct stat st;
  FILE *fd = fopen(fname, "r");
  stat(fname, &st);
  char *src_buffer = new char[st.st_size];
  fread(src_buffer, sizeof(char), st.st_size, fd);
  fclose(fd);

  ObCompressor *comp = create_compressor("snappy_1.0");
  EXPECT_TRUE(NULL != comp);
  if (NULL != comp)
  {
    char *comp_buffer = new char[st.st_size + comp->get_max_overflow_size(st.st_size)];
    char *decomp_buffer = new char[st.st_size];
    int64_t ret_size = st.st_size + comp->get_max_overflow_size(st.st_size);

    EXPECT_EQ(ObCompressor::COM_E_OVERFLOW, comp->compress(src_buffer, st.st_size, comp_buffer, st.st_size, ret_size));

    EXPECT_EQ(ObCompressor::COM_E_NOERROR, comp->compress(src_buffer, st.st_size, comp_buffer, ret_size, ret_size));
    EXPECT_EQ(ObCompressor::COM_E_NOERROR, comp->decompress(comp_buffer, ret_size, decomp_buffer, st.st_size, ret_size));
    EXPECT_EQ(st.st_size, ret_size);
    EXPECT_EQ(0, memcmp(decomp_buffer, src_buffer, st.st_size));

    EXPECT_EQ(ObCompressor::COM_E_INVALID_PARAM, comp->compress(NULL, 1, NULL, 1, ret_size));
    EXPECT_EQ(ObCompressor::COM_E_INVALID_PARAM, comp->compress(src_buffer, 0, comp_buffer, 0, ret_size));
    EXPECT_EQ(ObCompressor::COM_E_INVALID_PARAM, comp->compress(src_buffer, -1, comp_buffer, -1, ret_size));
    EXPECT_EQ(ObCompressor::COM_E_INVALID_PARAM, comp->decompress(NULL, 1, NULL, 1, ret_size));
    EXPECT_EQ(ObCompressor::COM_E_INVALID_PARAM, comp->decompress(src_buffer, 0, comp_buffer, 0, ret_size));
    EXPECT_EQ(ObCompressor::COM_E_INVALID_PARAM, comp->decompress(src_buffer, -1, comp_buffer, -1, ret_size));

    // TODO return value of snappy compress/decompress, overflow
    EXPECT_EQ(ObCompressor::COM_E_INTERNALERROR, comp->decompress(comp_buffer, ret_size, decomp_buffer, st.st_size, ret_size));

    EXPECT_EQ(ObCompressor::COM_E_NOIMPL, comp->set_compress_block_size(st.st_size));
    EXPECT_EQ(ObCompressor::COM_E_NOIMPL, comp->set_compress_level(1));

    EXPECT_EQ(0, strcmp("snappy", comp->get_compressor_name()));

    EXPECT_EQ(((int64_t)1)<<48, comp->get_interface_ver());

    delete[] decomp_buffer;
    delete[] comp_buffer;
    destroy_compressor(comp);
  }
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
