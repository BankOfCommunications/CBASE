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
    extern char *pstrKey[ TEST_COUNT ];
    extern int32_t iFlag;
    /* a middle value */
    static const int32_t MIDDLE_BOUND = 0x0FFFFFFF;
    /* Create a random key */
    int32_t get_next_random_value( int32_t &value );
    /*{
      value = (rand() & 0x7FFFFFFF);
      return 0;
    }*/
    /* Create an increasing key */
    int32_t get_next_up_value( int32_t &value );
    /*{
      value ++;
      return 0;
    }*/
    /* Create a decreasing key */
    int32_t get_next_down_value( int32_t &value );
    /*{
      value --;
      return 0;
    }*/
    /* Create an intersecting key */
    int32_t get_next_intersect_value( int32_t &value );
    /*{
      if( value < MIDDLE_BOUND ){
        value = 2 * MIDDLE_BOUND + 1;
      }
      else{
        value = 2 * MIDDLE_BOUND - 1;
      }
      return 0;
    }*/
    void set_up();
    void tear_down();
    /* test insert random keys to Btree and get keys by sequence*/
    TEST(KeyBtree_Get_Char_Test, get_01_random_Put_up_Get)
    {
      set_up();
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0, iSuccess_tmp;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE);
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
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        iRet = btree.get( pstrKey[ iLoop ], iGet);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
          EXPECT_EQ(iGet, iLoop + 1);
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(iSuccess, btree.get_object_count() );
      EXPECT_EQ(iSuccess, iSuccess_tmp );
      EXPECT_EQ(iFail, 0);
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /* test insert random keys to Btree and get keys by reverse*/
    TEST(KeyBtree_Get_Char_Test, get_02_random_Put_down_Get)
    {
      set_up();
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0, iSuccess_tmp;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE);
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
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = TEST_COUNT - 1; iLoop > -1; iLoop --)
      {
        iRet = btree.get( pstrKey[ iLoop ], iGet);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
          EXPECT_EQ(iGet, iLoop + 1 );
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(iSuccess, btree.get_object_count() );
      EXPECT_EQ(iSuccess, iSuccess_tmp );
      EXPECT_EQ(iFail, 0);
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /* test insert random keys to Btree and get keys by intersection*/
    TEST(KeyBtree_Get_Char_Test, get_03_random_Put_intersect_Get)
    {
      set_up();
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0, iSuccess_tmp;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE);
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
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0, jLoop = TEST_COUNT - 1; iLoop <= jLoop ; iLoop ++, jLoop--)
      {
        iRet = btree.get( pstrKey[ iLoop ], iGet);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
          EXPECT_EQ(iGet, iLoop + 1 );
        }
        else
        {
          iFail ++;
        }
        if(iLoop < jLoop)
        {
          iRet = btree.get( pstrKey[ jLoop ], iGet);
          if(iRet == ERROR_CODE_OK)
          {
            iSuccess ++;
            EXPECT_EQ(iGet, jLoop + 1);
          }
          else
          {
            iFail ++;
          }
        }
      }
      EXPECT_EQ(iSuccess, btree.get_object_count() );
      EXPECT_EQ(iSuccess, iSuccess_tmp );
      EXPECT_EQ(iFail, 0);
      /*release resources*/
      tear_down();
      btree.clear();
    }

    /* test insert up keys to Btree and get keys by sequence*/
    TEST(KeyBtree_Get_Char_Test, get_04_up_Put_up_Get)
    {
      set_up();
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0, iSuccess_tmp;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE);
      /*inset keys to Btree*/
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        /* malloc */
        pstrKey[ iLoop ] = (char *)malloc(TEST_SIZE);
        /* Set the flag for free */
        iFlag = 1;
        if ( get_next_up_value( iValue ) ) break;
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
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        iRet = btree.get( pstrKey[ iLoop ], iGet);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
          EXPECT_EQ(iGet, iLoop + 1);
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(iSuccess, btree.get_object_count() );
      EXPECT_EQ(iSuccess, iSuccess_tmp );
      EXPECT_EQ(iFail, 0);
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /* test insert up keys to Btree and get keys by reverse*/
    TEST(KeyBtree_Get_Char_Test, get_05_up_Put_down_Get)
    {
      set_up();
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0, iSuccess_tmp;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE);
      /*inset keys to Btree*/
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        /* malloc */
        pstrKey[ iLoop ] = (char *)malloc(TEST_SIZE);
        /* Set the flag for free */
        iFlag = 1;
        if ( get_next_up_value( iValue ) ) break;
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
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = TEST_COUNT - 1; iLoop > -1; iLoop --)
      {
        iRet = btree.get( pstrKey[ iLoop ], iGet);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
          EXPECT_EQ(iGet, iLoop + 1);
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(iSuccess, btree.get_object_count() );
      EXPECT_EQ(iSuccess, iSuccess_tmp );
      EXPECT_EQ(iFail, 0);
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /* test insert up keys to Btree and get keys by intersection*/
    TEST(KeyBtree_Get_Char_Test, get_06_up_Put_intersect_Get)
    {
      set_up();
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0, iSuccess_tmp;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE);
      /*inset keys to Btree*/
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        /* malloc */
        pstrKey[ iLoop ] = (char *)malloc(TEST_SIZE);
        /* Set the flag for free */
        iFlag = 1;
        if ( get_next_up_value( iValue ) ) break;
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
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0, jLoop = TEST_COUNT - 1; iLoop <= jLoop ; iLoop ++, jLoop--)
      {
        iRet = btree.get( pstrKey[ iLoop ], iGet);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
          EXPECT_EQ(iGet, iLoop + 1);
        }
        else
        {
          iFail ++;
        }
        if(iLoop < jLoop)
        {
          iRet = btree.get( pstrKey[ jLoop ], iGet);
          if(iRet == ERROR_CODE_OK)
          {
            iSuccess ++;
            EXPECT_EQ(iGet, jLoop + 1);
          }
          else
          {
            iFail ++;
          }
        }
      }
      EXPECT_EQ(iSuccess, btree.get_object_count() );
      EXPECT_EQ(iSuccess, iSuccess_tmp );
      EXPECT_EQ(iFail, 0);
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /* test insert down keys to Btree and get keys by sequence*/
    TEST(KeyBtree_Get_Char_Test, get_07_down_Put_up_Get)
    {
      set_up();
      int32_t iRet = 0;
      int32_t iValue = 0x7FFFFFFF;
      int32_t iSuccess = 0, iSuccess_tmp;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE);
      /*inset keys to Btree*/
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        /* malloc */
        pstrKey[ iLoop ] = (char *)malloc(TEST_SIZE);
        /* Set the flag for free */
        iFlag = 1;
        if ( get_next_down_value( iValue ) ) break;
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
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        iRet = btree.get( pstrKey[ iLoop ], iGet);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
          EXPECT_EQ(iGet, iLoop + 1);
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(iSuccess, btree.get_object_count() );
      EXPECT_EQ(iSuccess, iSuccess_tmp );
      EXPECT_EQ(iFail, 0);
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /* test insert down keys to Btree and get keys by reverse*/
    TEST(KeyBtree_Get_Char_Test, get_08_down_Put_down_Get)
    {
      set_up();
      int32_t iRet = 0;
      int32_t iValue = 0x7FFFFFFF;
      int32_t iSuccess = 0, iSuccess_tmp;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE);
      /*inset keys to Btree*/
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        /* malloc */
        pstrKey[ iLoop ] = (char *)malloc(TEST_SIZE);
        /* Set the flag for free */
        iFlag = 1;
        if ( get_next_down_value( iValue ) ) break;
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
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = TEST_COUNT - 1; iLoop > -1; iLoop --)
      {
        iRet = btree.get( pstrKey[ iLoop ], iGet);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
          EXPECT_EQ( iGet, iLoop + 1);
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(iSuccess, btree.get_object_count() );
      EXPECT_EQ(iSuccess, iSuccess_tmp );
      EXPECT_EQ(iFail, 0);
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /* test insert random keys to Btree and get keys by intersection*/
    TEST(KeyBtree_Get_Char_Test, get_09_down_Put_intersect_Get)
    {
      set_up();
      int32_t iRet = 0;
      int32_t iValue = 0x7FFFFFFF;
      int32_t iSuccess = 0, iSuccess_tmp;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE);
      /*inset keys to Btree*/
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        /* malloc */
        pstrKey[ iLoop ] = (char *)malloc(TEST_SIZE);
        /* Set the flag for free */
        iFlag = 1;
        if ( get_next_down_value( iValue ) ) break;
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
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0, jLoop = TEST_COUNT - 1; iLoop <= jLoop ; iLoop ++, jLoop--)
      {
        iRet = btree.get( pstrKey[ iLoop ], iGet);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
          EXPECT_EQ(iGet, iLoop + 1);
        }
        else
        {
          iFail ++;
        }
        if(iLoop < jLoop)
        {
          iRet = btree.get( pstrKey[ jLoop ], iGet);
          if(iRet == ERROR_CODE_OK)
          {
            iSuccess ++;
            EXPECT_EQ(iGet, jLoop + 1);
          }
          else
          {
            iFail ++;
          }
        }
      }
      EXPECT_EQ(iSuccess, btree.get_object_count() );
      EXPECT_EQ(iSuccess, iSuccess_tmp );
      EXPECT_EQ(iFail, 0);
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /* test insert intersect  keys to Btree and get keys by sequence*/
    TEST(KeyBtree_Get_Char_Test, get_10_intersec_Put_up_Get)
    {
      set_up();
      int32_t iRet = 0;
      int32_t iValue = 0x0FFFFFFF;
      int32_t iSuccess = 0, iSuccess_tmp;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE);
      /*inset keys to Btree*/
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        /* malloc */
        pstrKey[ iLoop ] = (char *)malloc(TEST_SIZE);
        /* Set the flag for free */
        iFlag = 1;
        if ( get_next_intersect_value( iValue ) ) break;
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
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        iRet = btree.get( pstrKey[ iLoop ], iGet);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
          EXPECT_EQ(iGet, iLoop + 1);
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(iSuccess, btree.get_object_count() );
      EXPECT_EQ(iSuccess, iSuccess_tmp );
      EXPECT_EQ(iFail, 0);
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /* test insert intersect keys to Btree and get keys by reverse*/
    TEST(KeyBtree_Get_Char_Test, get_11_intersect_Put_down_Get)
    {
      set_up();
      int32_t iRet = 0;
      int32_t iValue = 0x0FFFFFFF;
      int32_t iSuccess = 0, iSuccess_tmp;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE);
      /*inset keys to Btree*/
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        /* malloc */
        pstrKey[ iLoop ] = (char *)malloc(TEST_SIZE);
        /* Set the flag for free */
        iFlag = 1;
        if ( get_next_intersect_value( iValue ) ) break;
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
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = TEST_COUNT - 1; iLoop > -1; iLoop --)
      {
        iRet = btree.get( pstrKey[ iLoop ], iGet);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
          EXPECT_EQ(iGet, iLoop + 1);
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(iSuccess, btree.get_object_count() );
      EXPECT_EQ(iSuccess, iSuccess_tmp );
      EXPECT_EQ(iFail, 0);
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /* test insert intersect keys to Btree and get keys by intersection*/
    TEST(KeyBtree_Get_Char_Test, get_12_intersect_Put_intersect_Get)
    {
      set_up();
      int32_t iRet = 0;
      int32_t iValue = 0x0FFFFFFF;
      int32_t iSuccess = 0, iSuccess_tmp;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree( MAX_SIZE);
      /*inset keys to Btree*/
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        /* malloc */
        pstrKey[ iLoop ] = (char *)malloc(TEST_SIZE);
        /* Set the flag for free */
        iFlag = 1;
        if ( get_next_intersect_value( iValue ) ) break;
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
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0, jLoop = TEST_COUNT - 1; iLoop <= jLoop ; iLoop ++, jLoop--)
      {
        iRet = btree.get( pstrKey[ iLoop ], iGet);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
          EXPECT_EQ(iGet, iLoop + 1);
        }
        else
        {
          iFail ++;
        }
        if(iLoop < jLoop)
        {
          iRet = btree.get( pstrKey[ jLoop ], iGet);
          if(iRet == ERROR_CODE_OK)
          {
            iSuccess ++;
            EXPECT_EQ(iGet, jLoop + 1);
          }
          else
          {
            iFail ++;
          }
        }
      }
      EXPECT_EQ(iSuccess, btree.get_object_count() );
      EXPECT_EQ(iSuccess, iSuccess_tmp );
      EXPECT_EQ(iFail, 0);
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /**
     * test get a key multipy times from Btree
     */
    TEST(KeyBtree_Get_Char_Test, get_13_samekey)
    {
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0;
      int32_t iFail = 0;
      int32_t iGet = -1;
      char * key = NULL;
      charBtree btree( MAX_SIZE );

      /* Create the key */
      get_next_random_value(iValue);
      key = (char *)malloc(TEST_SIZE);
      sprintf( key, "%08x", iValue );
      /* insert a key to Btree */
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        /* Put the key-value pair */
        iRet = btree.put( key, iLoop + 1 );
        if (iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ( iFail, TEST_COUNT - 1 );
      EXPECT_EQ( iSuccess, btree.get_object_count() );
      EXPECT_EQ( iSuccess, 1);

      /* Get the key-value */
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        iRet = btree.get( key, iGet);
        if (iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
          EXPECT_EQ(iGet, 1);
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ( 1, btree.get_object_count() );
      EXPECT_EQ( iFail, 0 );
      EXPECT_EQ( iSuccess, TEST_COUNT );
      free(key);
      btree.clear();
    }
    /**
     * test get a key which the Btree don't have
     */
    TEST(KeyBtree_Get_Char_Test, get_14_nullkey)
    {
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0;
      int32_t iFail = 0;
      int32_t iGet = -1;
      char * key = NULL;
      charBtree btree( MAX_SIZE );

      /* Create the key */
      get_next_random_value(iValue);
      key = (char *)malloc(TEST_SIZE);
      sprintf( key, "%08x", iValue );
      /* insert no key to Btree */
      EXPECT_EQ( 0, btree.get_object_count() );

      /* Get the key-value */
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        iRet = btree.get( key, iGet);
        if (iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ( 0, btree.get_object_count() );
      EXPECT_EQ( iFail, TEST_COUNT  );
      EXPECT_EQ( iSuccess, 0);
      free(key);
    }
    /* test insert long keys to Btree */
    TEST(KeyBtree_Get_Char_Test, get_15_longkey)
    {
      set_up();
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0, iSuccess_tmp;
      int32_t iFail = 0;
      int32_t iGet = -1;
      charBtree btree(sizeof(char*));
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
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        iRet = btree.get( pstrKey[ iLoop ], iGet);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
          EXPECT_EQ(iGet, iLoop + 1);
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(iSuccess, btree.get_object_count() );
      EXPECT_EQ(iSuccess, iSuccess_tmp );
      EXPECT_EQ(iFail, 0);
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /*void set_up()
    {
       Reset the flag
      iFlag = 0;
      srand(time(NULL));
    }
    */
    /*
    void tear_down()
    {

      if (iFlag == 1)
      {
        for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
        {
          free( pstrKey[ iLoop ] );
        }
      }
    }*/
  }//end common
}//end oceanbase
