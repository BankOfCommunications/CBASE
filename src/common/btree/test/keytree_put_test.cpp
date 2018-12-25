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
    typedef KeyBtree<char *, int32_t> charBtree;
    /* List of key's pointer */
    char *pstrKey[ TEST_COUNT ];
    /* Flag for free */
    int32_t iFlag = 0;
    /* a middle bound value*/
    static const int32_t MIDDLE_BOUND = 0x0FFFFFFF;
    /* Create a random key */
    int32_t get_next_random_value( int32_t &value )
    {
      value = (rand() & 0x7FFFFFFF);
      return 0;
    }
    /* Create an increase key */
    int32_t get_next_up_value(int32_t &value)
    {
      value ++;
      return 0;
    }
    /* Create a decrease key */
    int32_t get_next_down_value(int32_t &value)
    {
      value --;
      return 0;
    }
    /* Create an intersect key */
    int32_t get_next_intersect_value(int32_t &value)
    {
      if(value < MIDDLE_BOUND)
      {
        value = 2 * MIDDLE_BOUND + 1;
      }
      else
      {
        value = 2 * MIDDLE_BOUND - 1;
      }
      return 0;
    }
    /* set up*/
    void set_up();
    /* free resources*/
    void tear_down();
    /*
        typedef KeyBtree<TestKey, int32_t> StringBtree;
        typedef KeyBtree<TestKey, char *> StringBtreeStr;*/
#if 0
    /* Overload the operator - */
    int32_t operator- (char *keyA, char *keyB)
    {
      int32_t iSizeA = 0;
      int32_t iSizeB = 0;
      int32_t iLoop = 0;
      iSizeA = strlen( keyA );
      iSizeB = strlen( keyB );
      if (iSizeA > TEST_SIZE)
      {
        iSizeA = TEST_SIZE;
      }
      if (iSizeB > TEST_SIZE)
      {
        iSizeB = TEST_SIZE;
      }
      for (iLoop = 0; iLoop < iSizeA && iLoop < iSizeB; iLoop ++)
      {
        if (keyA[ iLoop ] < keyB[ iLoop ])
        {
          return -1;
        }
        else if (keyA[ iLoop ] > keyB[ iLoop ])
        {
          return 1;
        }
        else
        {
          continue;
        }
      }
      return (iSizeA - iSizeB);
    }
#endif

    /**
     * test put random keys into Btree
     */
    TEST(KeyBtree_Put_Char_Test, put_01_random)
    {
      set_up();
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE );

      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        /* malloc */
        pstrKey[ iLoop ] = (char *)malloc( TEST_SIZE );
        /* Set the flag for free */
        iFlag = 1;
        /* Create the key */
        get_next_random_value( iValue );
        sprintf( pstrKey[ iLoop ], "%08x", iValue );
        /* Put the key-value pair */
        iRet = btree.put( pstrKey[ iLoop ], iLoop + 1 );
        if (iRet == ERROR_CODE_OK)
        {
          iRet = btree.get( pstrKey[ iLoop ], iGet );
          EXPECT_EQ( iGet, iLoop + 1 );
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ( iFail, 0 );
      EXPECT_EQ( iSuccess, btree.get_object_count() );
      tear_down();
      btree.clear();
    }

    /**
     * test put increasing keys into Btree
     */
    TEST(KeyBtree_Put_Char_Test, put_02_up)
    {
      set_up();
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE );

      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        /* malloc */
        pstrKey[ iLoop ] = (char *)malloc( TEST_SIZE );
        /* Set the flag for free */
        iFlag = 1;
        /* Create the key */
        get_next_up_value( iValue );
        sprintf( pstrKey[ iLoop ], "%08x", iValue );
        /* Put the key-value pair */
        iRet = btree.put( pstrKey[ iLoop ], iLoop + 1 );
        if (iRet == ERROR_CODE_OK)
        {
          iRet = btree.get( pstrKey[ iLoop ], iGet );
          EXPECT_EQ( iGet, iLoop + 1 );
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ( iFail, 0 );
      EXPECT_EQ( iSuccess, btree.get_object_count() );
      tear_down();
      btree.clear();
    }

    /**
     * test put decreasing keys into Btree
     */
    TEST(KeyBtree_Put_Char_Test, put_03_down)
    {
      set_up();
      int32_t iRet = 0;
      int32_t iValue = 0x7FFFFFFF;
      int32_t iSuccess = 0;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE );

      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        /* malloc */
        pstrKey[ iLoop ] = (char *)malloc( TEST_SIZE );
        /* Set the flag for free */
        iFlag = 1;
        /* Create the key */
        get_next_down_value( iValue );
        sprintf( pstrKey[ iLoop ], "%08x", iValue );
        /* Put the key-value pair */
        iRet = btree.put( pstrKey[ iLoop ], iLoop + 1 );
        if (iRet == ERROR_CODE_OK)
        {
          iRet = btree.get( pstrKey[ iLoop ], iGet );
          EXPECT_EQ( iGet, iLoop + 1 );
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ( iFail, 0 );
      EXPECT_EQ( iSuccess, btree.get_object_count() );
      tear_down();
      btree.clear();
    }

    /**
     * test put intersecting keys into Btree
     */
    TEST(KeyBtree_Put_Char_Test, put_04_intersect)
    {
      set_up();
      int32_t iRet = 0;
      int32_t iValue = MIDDLE_BOUND;
      int32_t iSuccess = 0;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE );

      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        /* malloc */
        pstrKey[ iLoop ] = (char *)malloc( TEST_SIZE );
        /* Set the flag for free */
        iFlag = 1;
        /* Create the key */
        get_next_intersect_value( iValue );
        sprintf( pstrKey[ iLoop ], "%08x", iValue );
        /* Put the key-value pair */
        iRet = btree.put( pstrKey[ iLoop ], iLoop + 1 );
        if (iRet == ERROR_CODE_OK)
        {
          iRet = btree.get( pstrKey[ iLoop ], iGet );
          EXPECT_EQ( iGet, iLoop + 1 );
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ( iFail, 0 );
      EXPECT_EQ( iSuccess, btree.get_object_count() );
      tear_down();
      btree.clear();
    }

    /**
     * test put same keys into Btree,these keys have same content and defferent address
     */
    TEST(KeyBtree_Put_Char_Test, put_05_samevaluekey)
    {
      set_up();
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE );

      get_next_random_value(iValue);

      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        /* malloc */
        pstrKey[ iLoop ] = (char *)malloc( TEST_SIZE );
        /* Set the flag for free */
        iFlag = 1;
        /* Create the key */
        sprintf( pstrKey[ iLoop ], "%08x", iValue );
        /* Put the key-value pair */
        iRet = btree.put( pstrKey[ iLoop ], iLoop + 1 );
        if (iRet == ERROR_CODE_OK)
        {
          iRet = btree.get( pstrKey[ iLoop ], iGet );
          EXPECT_EQ( iGet, iLoop + 1 );
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ( iFail, 0);
      EXPECT_EQ( iSuccess, btree.get_object_count() );
      EXPECT_EQ( iSuccess, TEST_COUNT);
      tear_down();
      btree.clear();
    }

    /**
     * test put same keys into Btree,these keys have same content and same address
     */
    TEST(KeyBtree_Put_Char_Test, put_06_sameaddresskey)
    {
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0;
      int32_t iFail = 0;
      //int32_t iGet = -1;
      charBtree btree( MAX_SIZE );

      get_next_random_value(iValue);
      char * key = (char *)malloc( TEST_SIZE );
      sprintf( key, "%08x", iValue);
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        /* Put the key-value pair */
        iRet = btree.put( key, iLoop + 1 );
        if (((iRet == ERROR_CODE_OK) && (iLoop == 0)) || (iRet == ERROR_CODE_KEY_REPEAT))
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      free(key);
      EXPECT_EQ( iFail, 0);
      EXPECT_EQ( iSuccess, TEST_COUNT );
      EXPECT_EQ( 1, btree.get_object_count() );
      btree.clear();
    }

    /**
     *test put long keys into Btree
     */
    TEST(KeyBtree_Put_Char_Test, put_07_longkey)
    {
      set_up();
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0;
      int32_t iFail = 0;
      charBtree btree(sizeof(char*));

      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        /* malloc */
        pstrKey[ iLoop ] = (char *)malloc( TEST_SIZE );
        /* Set the flag for free */
        iFlag = 1;
        /* Create the key */
        get_next_random_value( iValue );
        sprintf( pstrKey[ iLoop ], "%08x", iValue );
        /* Put the key-value pair */
        iRet = btree.put( pstrKey[ iLoop ], iLoop + 1 );
        if (iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ( iFail, 0 );
      EXPECT_EQ( iSuccess, btree.get_object_count() );
      tear_down();
      btree.clear();
    }
    void set_up()
    {
      iFlag = 0;
    }
    void tear_down()
    {
      // printf("free\n");
      if(iFlag == 1)
      {
        for(int i = 0 ; i < TEST_COUNT; i++)
        {
          free(pstrKey[i]);
        }
      }
    }
  }//end of namespace common
}// end of namespace oceanbase
