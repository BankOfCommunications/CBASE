#include <limits.h>
#include <key_btree.h>
#include <gtest/gtest.h>
#include "test_key_str.h"

namespace oceanbase
{

  namespace common
  {

    /* The size of Key for Btree */
    static const int32_t MAX_SIZE = 16;
    static const int32_t MIN_SIZE = 1;
    /* The count of test */
    static const int32_t TEST_COUNT = 4000;
    /* The size of key for test */
    static const int32_t TEST_SIZE = 16;
    /* char * Btree */
    typedef KeyBtree<char*, int32_t> charBtree;
    /* List of key's pointer */
    extern char* pstrKey [ TEST_COUNT ];
    extern int32_t iFlag;
    /* a middle value */
    static const int32_t MIDDLE_BOUND = 0x0FFFFFFF;
    /* Create a random key */
    int32_t get_next_random_value( int32_t &value );

    /* Create an increasing key */
    int32_t get_next_up_value( int32_t &value );

    /* Create a decreasing key */
    int32_t get_next_down_value( int32_t &value );

    /* Create an intersecting key */
    int32_t get_next_intersect_value( int32_t &value );

    /* set up */
    void set_up();

    /* release resources*/
    void tear_down();

    /* test insert keys to Btree and get next value between included minkey and included maxkey */
    TEST(KeyBtree_GetNext_Char_Test, getnext_01_include_minkey_include_maxkey_getnext)
    {
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0, iSuccess_tmp = 0;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE);
      char ** fromkey = NULL;
      char ** endkey = NULL;
      /*inset keys to Btree*/
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        /* malloc */
        pstrKey[ iLoop ] = (char *)malloc(TEST_SIZE);
        /* Set the flag for free */
        iFlag = 1;
        if ( get_next_random_value( iValue ) ) break;
        /* Set the key */
        sprintf( pstrKey[ iLoop ], "%08x", iValue);
        /* Put the key-value pair */
        iRet = btree.put( pstrKey[ iLoop ], iLoop + 1 );
        if (iRet == ERROR_CODE_OK)
        {
          iRet = btree.get( pstrKey[ iLoop ], iGet);
          iSuccess ++;
          EXPECT_EQ( iGet, iLoop + 1 );
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ( iFail, 0);
      EXPECT_EQ( iSuccess, btree.get_object_count() );
      /*get keys form Btree*/
      fromkey = btree.get_min_key();
      endkey =  btree.get_max_key();
      // printf("%d %d\n",*fromkey,*endkey);
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      {
        BtreeReadHandle handle;
        if( btree.get_read_handle(handle) == ERROR_CODE_OK )
        {
          btree.set_key_range(handle, fromkey, 0, endkey, 0);
          while(btree.get_next(handle, iGet) == ERROR_CODE_OK)
          {
            iSuccess ++;
          }
        }
      }
      EXPECT_EQ( iSuccess, iSuccess_tmp );
      EXPECT_EQ( iSuccess_tmp, btree.get_object_count() );
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /* test insert keys to Btree and get next value between included minkey and excluded maxkey */
    TEST(KeyBtree_GetNext_Char_Test, getnext_02_include_minkey_exclude_maxkey_getnext)
    {
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0, iSuccess_tmp = 0;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE);
      char ** fromkey = NULL;
      char ** endkey = NULL;
      /*inset keys to Btree*/
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        /* malloc */
        pstrKey[ iLoop ] = (char *)malloc(TEST_SIZE);
        /* Set the flag for free */
        iFlag = 1;
        if ( get_next_random_value( iValue ) ) break;
        /* Set the key */
        sprintf( pstrKey[ iLoop ], "%08x", iValue);
        /* Put the key-value pair */
        iRet = btree.put( pstrKey[ iLoop ], iLoop + 1 );
        if (iRet == ERROR_CODE_OK)
        {
          iRet = btree.get( pstrKey[ iLoop ], iGet);
          iSuccess ++;
          EXPECT_EQ( iGet, iLoop + 1 );
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ( iFail, 0);
      EXPECT_EQ( iSuccess, btree.get_object_count() );
      /*get keys form Btree*/
      fromkey = btree.get_min_key();
      endkey =  btree.get_max_key();
      // printf("%d %d\n",*fromkey,*endkey);
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      {
        BtreeReadHandle handle;
        if( btree.get_read_handle(handle) == ERROR_CODE_OK )
        {
          btree.set_key_range(handle, fromkey, 0, endkey, 1);
          while(btree.get_next(handle, iGet) == ERROR_CODE_OK)
          {
            iSuccess ++;
          }
        }
      }
      EXPECT_EQ( iSuccess, iSuccess_tmp );
      EXPECT_EQ( iSuccess_tmp, btree.get_object_count() );
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /* test insert keys to Btree and get next value between excluded minkey and included maxkey */
    TEST(KeyBtree_GetNext_Char_Test, getnext_03_exclude_minkey_include_maxkey_getnext)
    {
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0, iSuccess_tmp = 0;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE);
      char ** fromkey = NULL;
      char ** endkey = NULL;
      /*inset keys to Btree*/
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        /* malloc */
        pstrKey[ iLoop ] = (char *)malloc(TEST_SIZE);
        /* Set the flag for free */
        iFlag = 1;
        if ( get_next_random_value( iValue ) ) break;
        /* Set the key */
        sprintf( pstrKey[ iLoop ], "%08x", iValue);
        /* Put the key-value pair */
        iRet = btree.put( pstrKey[ iLoop ], iLoop + 1 );
        if (iRet == ERROR_CODE_OK)
        {
          iRet = btree.get( pstrKey[ iLoop ], iGet);
          iSuccess ++;
          EXPECT_EQ( iGet, iLoop + 1 );
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ( iFail, 0);
      EXPECT_EQ( iSuccess, btree.get_object_count() );
      /*get keys form Btree*/
      fromkey = btree.get_min_key();
      endkey =  btree.get_max_key();
      // printf("%d %d\n",*fromkey,*endkey);
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      {
        BtreeReadHandle handle;
        if( btree.get_read_handle(handle) == ERROR_CODE_OK )
        {
          btree.set_key_range(handle, fromkey, 1, endkey, 0);
          while(btree.get_next(handle, iGet) == ERROR_CODE_OK)
          {
            iSuccess ++;
          }
        }
      }
      EXPECT_EQ( iSuccess, iSuccess_tmp );
      EXPECT_EQ( iSuccess_tmp, btree.get_object_count() );
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /* test insert keys to Btree and get next value between excluded minkey and excluded maxkey */
    TEST(KeyBtree_GetNext_Char_Test, getnext_04_exclude_minkey_exclude_maxkey_getnext)
    {
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0, iSuccess_tmp = 0;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE);
      char ** fromkey = NULL;
      char ** endkey = NULL;
      /*inset keys to Btree*/
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        /* malloc */
        pstrKey[ iLoop ] = (char *)malloc(TEST_SIZE);
        /* Set the flag for free */
        iFlag = 1;
        if ( get_next_random_value( iValue ) ) break;
        /* Set the key */
        sprintf( pstrKey[ iLoop ], "%08x", iValue);
        /* Put the key-value pair */
        iRet = btree.put( pstrKey[ iLoop ], iLoop + 1 );
        if (iRet == ERROR_CODE_OK)
        {
          iRet = btree.get( pstrKey[ iLoop ], iGet);
          iSuccess ++;
          EXPECT_EQ( iGet, iLoop + 1 );
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ( iFail, 0);
      EXPECT_EQ( iSuccess, btree.get_object_count() );
      /*get keys form Btree*/
      fromkey = btree.get_min_key();
      endkey =  btree.get_max_key();
      // printf("%d %d\n",*fromkey,*endkey);
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      {
        BtreeReadHandle handle;
        if( btree.get_read_handle(handle) == ERROR_CODE_OK )
        {
          btree.set_key_range(handle, fromkey, 1, endkey, 1);
          while(btree.get_next(handle, iGet) == ERROR_CODE_OK)
          {
            iSuccess ++;
          }
        }
      }
      EXPECT_EQ( iSuccess, iSuccess_tmp );
      EXPECT_EQ( iSuccess_tmp, btree.get_object_count() );
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /* test insert keys to Btree and get next value between included maxkey and included minkey */
    TEST(KeyBtree_GetNext_Char_Test, getnext_05_include_maxkey_include_minkey_getnext)
    {
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0, iSuccess_tmp = 0;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE);
      char ** fromkey = NULL;
      char ** endkey = NULL;
      /*inset keys to Btree*/
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        /* malloc */
        pstrKey[ iLoop ] = (char *)malloc(TEST_SIZE);
        /* Set the flag for free */
        iFlag = 1;
        if ( get_next_random_value( iValue ) ) break;
        /* Set the key */
        sprintf( pstrKey[ iLoop ], "%08x", iValue);
        /* Put the key-value pair */
        iRet = btree.put( pstrKey[ iLoop ], iLoop + 1 );
        if (iRet == ERROR_CODE_OK)
        {
          iRet = btree.get( pstrKey[ iLoop ], iGet);
          iSuccess ++;
          EXPECT_EQ( iGet, iLoop + 1 );
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ( iFail, 0);
      EXPECT_EQ( iSuccess, btree.get_object_count() );
      /*get keys form Btree*/
      endkey = btree.get_min_key();
      fromkey =  btree.get_max_key();
      // printf("%d %d\n",*fromkey,*endkey);
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      {
        BtreeReadHandle handle;
        if( btree.get_read_handle(handle) == ERROR_CODE_OK )
        {
          btree.set_key_range(handle, fromkey, 0, endkey, 0);
          while(btree.get_next(handle, iGet) == ERROR_CODE_OK)
          {
            iSuccess ++;
          }
        }
      }
      EXPECT_EQ( iSuccess, btree.get_object_count());
      EXPECT_EQ( iSuccess_tmp, btree.get_object_count() );
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /* test insert keys to Btree and get next value between excluded maxkey and included minkey */
    TEST(KeyBtree_GetNext_Char_Test, getnext_06_exclude_maxkey_include_minkey_getnext)
    {
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0, iSuccess_tmp = 0;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE);
      char ** fromkey = NULL;
      char ** endkey = NULL;
      /*inset keys to Btree*/
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        /* malloc */
        pstrKey[ iLoop ] = (char *)malloc(TEST_SIZE);
        /* Set the flag for free */
        iFlag = 1;
        if ( get_next_random_value( iValue ) ) break;
        /* Set the key */
        sprintf( pstrKey[ iLoop ], "%08x", iValue);
        /* Put the key-value pair */
        iRet = btree.put( pstrKey[ iLoop ], iLoop + 1 );
        if (iRet == ERROR_CODE_OK)
        {
          iRet = btree.get( pstrKey[ iLoop ], iGet);
          iSuccess ++;
          EXPECT_EQ( iGet, iLoop + 1 );
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ( iFail, 0);
      EXPECT_EQ( iSuccess, btree.get_object_count() );
      /*get keys form Btree*/
      endkey = btree.get_min_key();
      fromkey =  btree.get_max_key();
      // printf("%d %d\n",*fromkey,*endkey);
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      {
        BtreeReadHandle handle;
        if( btree.get_read_handle(handle) == ERROR_CODE_OK )
        {
          btree.set_key_range(handle, fromkey, 1, endkey, 0);
          while(btree.get_next(handle, iGet) == ERROR_CODE_OK)
          {
            iSuccess ++;
          }
        }
      }
      EXPECT_EQ( iSuccess, btree.get_object_count() );
      EXPECT_EQ( iSuccess_tmp, btree.get_object_count() );
      /*release resources*/
      tear_down();
      btree.clear();
    }

    /* test insert keys to Btree and get next value between included maxkey and excluded minkey */
    TEST(KeyBtree_GetNext_Char_Test, getnext_07_include_maxkey_exclude_minkey_getnext)
    {
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0, iSuccess_tmp = 0;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE);
      char ** fromkey = NULL;
      char ** endkey = NULL;
      /*inset keys to Btree*/
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        /* malloc */
        pstrKey[ iLoop ] = (char *)malloc(TEST_SIZE);
        /* Set the flag for free */
        iFlag = 1;
        if ( get_next_random_value( iValue ) ) break;
        /* Set the key */
        sprintf( pstrKey[ iLoop ], "%08x", iValue);
        /* Put the key-value pair */
        iRet = btree.put( pstrKey[ iLoop ], iLoop + 1 );
        if (iRet == ERROR_CODE_OK)
        {
          iRet = btree.get( pstrKey[ iLoop ], iGet);
          iSuccess ++;
          EXPECT_EQ( iGet, iLoop + 1 );
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ( iFail, 0);
      EXPECT_EQ( iSuccess, btree.get_object_count() );
      /*get keys form Btree*/
      endkey = btree.get_min_key();
      fromkey =  btree.get_max_key();
      // printf("%d %d\n",*fromkey,*endkey);
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      {
        BtreeReadHandle handle;
        if( btree.get_read_handle(handle) == ERROR_CODE_OK )
        {
          btree.set_key_range(handle, fromkey, 0, endkey, 1);
          while(btree.get_next(handle, iGet) == ERROR_CODE_OK)
          {
            iSuccess ++;
          }
        }
      }
      EXPECT_EQ( iSuccess, btree.get_object_count() );
      EXPECT_EQ( iSuccess_tmp, btree.get_object_count() );
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /* test insert keys to Btree and get next value between excluded maxkey and excluded minkey */
    TEST(KeyBtree_GetNext_Char_Test, getnext_08_exclude_maxkey_exclude_minkey_getnext)
    {
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0, iSuccess_tmp = 0;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE);
      char ** fromkey = NULL;
      char ** endkey = NULL;
      /*inset keys to Btree*/
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        /* malloc */
        pstrKey[ iLoop ] = (char *)malloc(TEST_SIZE);
        /* Set the flag for free */
        iFlag = 1;
        if ( get_next_random_value( iValue ) ) break;
        /* Set the key */
        sprintf( pstrKey[ iLoop ], "%08x", iValue);
        /* Put the key-value pair */
        iRet = btree.put( pstrKey[ iLoop ], iLoop + 1 );
        if (iRet == ERROR_CODE_OK)
        {
          iRet = btree.get( pstrKey[ iLoop ], iGet);
          iSuccess ++;
          EXPECT_EQ( iGet, iLoop + 1 );
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ( iFail, 0);
      EXPECT_EQ( iSuccess, btree.get_object_count() );
      /*get keys form Btree*/
      endkey = btree.get_min_key();
      fromkey =  btree.get_max_key();
      // printf("%d %d\n",*fromkey,*endkey);
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      {
        BtreeReadHandle handle;
        if( btree.get_read_handle(handle) == ERROR_CODE_OK )
        {
          btree.set_key_range(handle, fromkey, 1, endkey, 1);
          while(btree.get_next(handle, iGet) == ERROR_CODE_OK)
          {
            iSuccess ++;
          }
        }
      }
      EXPECT_EQ( iSuccess, btree.get_object_count() );
      EXPECT_EQ( iSuccess_tmp, btree.get_object_count() );
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /* test insert one key to Btree and get next value between included onekey and included onekey */
    TEST(KeyBtree_GetNext_Char_Test, getnext_09_include_onekey_include_onekey_getnext)
    {
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0, iSuccess_tmp = 0;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE);
      char ** fromkey = NULL;
      char ** endkey = NULL;
      char * key = NULL;
      /* get value and key */
      key = (char *)malloc(TEST_SIZE);
      get_next_random_value(iValue);
      sprintf(key, "%08x", iValue);
      iRet = btree.put(key, iValue);
      if( iRet == ERROR_CODE_OK )
      {
        iSuccess ++;
      }
      else
      {
        iFail ++;
      }
      EXPECT_EQ( iFail, 0);
      EXPECT_EQ( 1, btree.get_object_count() );
      EXPECT_EQ( iSuccess, 1);

      //fromkey = btree.get_min_key();
      //endkey = btree.get_max_key();
      fromkey = &key;
      endkey = &key;
      /* test get next keys */
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      {
        BtreeReadHandle handle;
        if( btree.get_read_handle(handle) == ERROR_CODE_OK )
        {
          btree.set_key_range(handle, fromkey, 0, endkey, 0);
          while(btree.get_next(handle, iGet) == ERROR_CODE_OK)
          {
            iSuccess ++;
          }
        }
      }
      EXPECT_EQ( iSuccess, 1 );
      EXPECT_EQ( iSuccess_tmp, btree.get_object_count() );
      /*release resources*/
      free(key);
      btree.clear();
    }
    /* test insert one key to Btree and get next value between included onekey and excluded onekey */
    TEST(KeyBtree_GetNext_Char_Test, getnext_10_include_onekey_exclude_onekey_getnext)
    {
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0, iSuccess_tmp;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE);
      char ** fromkey = NULL;
      char ** endkey = NULL;
      char * key = (char *)malloc(TEST_SIZE);
      /* get value and key */
      get_next_random_value(iValue);
      sprintf(key, "%08x", iValue);
      iRet = btree.put(key, iValue);
      if( iRet == ERROR_CODE_OK )
      {
        iSuccess ++;
      }
      else
      {
        iFail ++;
      }
      EXPECT_EQ( iFail, 0);
      EXPECT_EQ( 1, btree.get_object_count() );
      EXPECT_EQ( iSuccess, 1);

      fromkey = &key;
      endkey = &key;
      /* test get next keys */
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      {
        BtreeReadHandle handle;
        if( btree.get_read_handle(handle) == ERROR_CODE_OK )
        {
          btree.set_key_range(handle, fromkey, 0, endkey, 1);
          while(btree.get_next(handle, iGet) == ERROR_CODE_OK)
          {
            iSuccess ++;
          }
        }
      }
      EXPECT_EQ( iSuccess, 0 );
      EXPECT_EQ( iSuccess_tmp, btree.get_object_count() );
      /*release resources*/
      free(key);
      btree.clear();
    }
    /* test insert one key to Btree and get next value between excluded onekey and included onekey */
    TEST(KeyBtree_GetNext_Char_Test, getnext_11_exclude_onekey_include_onekey_getnext)
    {
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0, iSuccess_tmp;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE);
      char ** fromkey = NULL;
      char ** endkey = NULL;
      char * key = (char *)malloc(TEST_SIZE);
      /* get value and key */
      get_next_random_value(iValue);
      sprintf(key, "%08x", iValue);
      iRet = btree.put(key, iValue);
      if( iRet == ERROR_CODE_OK )
      {
        iSuccess ++;
      }
      else
      {
        iFail ++;
      }
      EXPECT_EQ( iFail, 0);
      EXPECT_EQ( 1, btree.get_object_count() );
      EXPECT_EQ( iSuccess, 1);

      fromkey = &key;
      endkey = &key;
      /* test get next keys */
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      {
        BtreeReadHandle handle;
        if( btree.get_read_handle(handle) == ERROR_CODE_OK )
        {
          btree.set_key_range(handle, fromkey, 1, endkey, 0);
          while(btree.get_next(handle, iGet) == ERROR_CODE_OK)
          {
            iSuccess ++;
          }
        }
      }
      EXPECT_EQ( iSuccess, 0 );
      EXPECT_EQ( iSuccess_tmp, btree.get_object_count() );
      /*release resources*/
      free(key);
      btree.clear();
    }
    /* test insert one key to Btree and get next value between excluded onekey and excluded onekey */
    TEST(KeyBtree_GetNext_Char_Test, getnext_12_exclude_onekey_exclude_onekey_getnext)
    {
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0, iSuccess_tmp;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE);
      char ** fromkey = NULL;
      char ** endkey = NULL;
      char * key = (char *)malloc(TEST_SIZE);
      /* get value and key */
      get_next_random_value(iValue);
      sprintf(key, "%08x", iValue);
      iRet = btree.put(key, iValue);
      if( iRet == ERROR_CODE_OK )
      {
        iSuccess ++;
      }
      else
      {
        iFail ++;
      }
      EXPECT_EQ( iFail, 0);
      EXPECT_EQ( 1, btree.get_object_count() );
      EXPECT_EQ( iSuccess, 1);

      fromkey = &key;
      endkey = &key;
      /* test get next keys */
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      {
        BtreeReadHandle handle;
        if( btree.get_read_handle(handle) == ERROR_CODE_OK )
        {
          btree.set_key_range(handle, fromkey, 1, endkey, 1);
          while(btree.get_next(handle, iGet) == ERROR_CODE_OK)
          {
            iSuccess ++;
          }
        }
      }
      EXPECT_EQ( iSuccess, 0 );
      EXPECT_EQ( iSuccess_tmp, btree.get_object_count() );
      /*release resources*/
      free(key);
      btree.clear();
    }
    /* test insert one key to Btree and get next value between included one_min_key and included one_max_key */
    TEST(KeyBtree_GetNext_Char_Test, getnext_13_include_oneminkey_include_onemaxkey_getnext)
    {
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0, iSuccess_tmp = 0;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE);
      char ** fromkey = NULL;
      char ** endkey = NULL;
      char * key = NULL;
      /* get value and key */
      key = (char *)malloc(TEST_SIZE);
      get_next_random_value(iValue);
      sprintf(key, "%08x", iValue);
      iRet = btree.put(key, iValue);
      if( iRet == ERROR_CODE_OK )
      {
        iSuccess ++;
      }
      else
      {
        iFail ++;
      }
      EXPECT_EQ( iFail, 0);
      EXPECT_EQ( 1, btree.get_object_count() );
      EXPECT_EQ( iSuccess, 1);
      /* here use get_min_key and get_max_key */
      fromkey = btree.get_min_key();
      endkey = btree.get_max_key();
      //fromkey = &key;
      //endkey =  &key;
      /* test get next keys */
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      {
        BtreeReadHandle handle;
        if( btree.get_read_handle(handle) == ERROR_CODE_OK )
        {
          btree.set_key_range(handle, fromkey, 0, endkey, 0);
          while(btree.get_next(handle, iGet) == ERROR_CODE_OK)
          {
            iSuccess ++;
          }
        }
      }
      EXPECT_EQ( iSuccess, 1 );
      EXPECT_EQ( iSuccess_tmp, btree.get_object_count() );
      /*release resources*/
      free(key);
      btree.clear();
    }
    /* test insert one key to Btree and get next value between included one_min_key and excluded one_max_key */
    TEST(KeyBtree_GetNext_Char_Test, getnext_14_include_oneminkey_exclude_onemaxkey_getnext)
    {
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0, iSuccess_tmp = 0;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE);
      char ** fromkey = NULL;
      char ** endkey = NULL;
      char * key = NULL;
      /* get value and key */
      key = (char *)malloc(TEST_SIZE);
      get_next_random_value(iValue);
      sprintf(key, "%08x", iValue);
      iRet = btree.put(key, iValue);
      if( iRet == ERROR_CODE_OK )
      {
        iSuccess ++;
      }
      else
      {
        iFail ++;
      }
      EXPECT_EQ( iFail, 0);
      EXPECT_EQ( 1, btree.get_object_count() );
      EXPECT_EQ( iSuccess, 1);
      /* here use get_min_key and get_max_key */
      fromkey = btree.get_min_key();
      endkey = btree.get_max_key();
      //fromkey = &key;
      //endkey =  &key;
      /* test get next keys */
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      {
        BtreeReadHandle handle;
        if( btree.get_read_handle(handle) == ERROR_CODE_OK )
        {
          btree.set_key_range(handle, fromkey, 0, endkey, 1);
          while(btree.get_next(handle, iGet) == ERROR_CODE_OK)
          {
            iSuccess ++;
          }
        }
      }
      EXPECT_EQ( iSuccess, btree.get_object_count() );
      EXPECT_EQ( iSuccess_tmp, btree.get_object_count() );
      /*release resources*/
      free(key);
      btree.clear();
    }

    /* test insert one key to Btree and get next value between excluded one_min_key and included one_max_key */
    TEST(KeyBtree_GetNext_Char_Test, getnext_15_exclude_oneminkey_include_onemaxkey_getnext)
    {
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0, iSuccess_tmp = 0;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE);
      char ** fromkey = NULL;
      char ** endkey = NULL;
      char * key = NULL;
      /* get value and key */
      key = (char *)malloc(TEST_SIZE);
      get_next_random_value(iValue);
      sprintf(key, "%08x", iValue);
      iRet = btree.put(key, iValue);
      if( iRet == ERROR_CODE_OK )
      {
        iSuccess ++;
      }
      else
      {
        iFail ++;
      }
      EXPECT_EQ( iFail, 0);
      EXPECT_EQ( 1, btree.get_object_count() );
      EXPECT_EQ( iSuccess, 1);
      /* here use get_min_key and get_max_key */
      fromkey = btree.get_min_key();
      endkey = btree.get_max_key();
      //fromkey = &key;
      //endkey =  &key;
      /* test get next keys */
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      {
        BtreeReadHandle handle;
        if( btree.get_read_handle(handle) == ERROR_CODE_OK )
        {
          btree.set_key_range(handle, fromkey, 1, endkey, 0);
          while(btree.get_next(handle, iGet) == ERROR_CODE_OK)
          {
            iSuccess ++;
          }
        }
      }
      EXPECT_EQ( iSuccess, btree.get_object_count());
      EXPECT_EQ( iSuccess_tmp, btree.get_object_count() );
      /*release resources*/
      free(key);
      btree.clear();
    }

    /* test insert one key to Btree and get next value between excluded one_min_key and excluded one_max_key */
    TEST(KeyBtree_GetNext_Char_Test, getnext_16_exclude_oneminkey_exclude_onemaxkey_getnext)
    {
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0, iSuccess_tmp = 0;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE);
      char ** fromkey = NULL;
      char ** endkey = NULL;
      char * key = NULL;
      /* get value and key */
      key = (char *)malloc(TEST_SIZE);
      get_next_random_value(iValue);
      sprintf(key, "%08x", iValue);
      iRet = btree.put(key, iValue);
      if( iRet == ERROR_CODE_OK )
      {
        iSuccess ++;
      }
      else
      {
        iFail ++;
      }
      EXPECT_EQ( iFail, 0);
      EXPECT_EQ( 1, btree.get_object_count() );
      EXPECT_EQ( iSuccess, 1);
      /* here use get_min_key and get_max_key */
      fromkey = btree.get_min_key();
      endkey = btree.get_max_key();
      //fromkey = &key;
      //endkey =  &key;
      /* test get next keys */
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      {
        BtreeReadHandle handle;
        if( btree.get_read_handle(handle) == ERROR_CODE_OK )
        {
          btree.set_key_range(handle, fromkey, 1, endkey, 1);
          while(btree.get_next(handle, iGet) == ERROR_CODE_OK)
          {
            iSuccess ++;
          }
        }
      }
      EXPECT_EQ( iSuccess, btree.get_object_count());
      EXPECT_EQ( iSuccess_tmp, btree.get_object_count() );
      /*release resources*/
      free(key);
      btree.clear();
    }
  }//end of common
}//end of oceanbase
