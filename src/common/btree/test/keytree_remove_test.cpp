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
    extern int32_t iFlag ;
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
    void set_up();
    void tear_down();
    /* test insert random keys to Btree and remove keys by sequence*/
    TEST(KeyBtree_Remove_Char_Test, remove_01_random_Put_up_Remove)
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
      /*remove keys form Btree*/
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        iRet = btree.remove( pstrKey[ iLoop ]);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(0, btree.get_object_count() );
      EXPECT_EQ(iSuccess, iSuccess_tmp );
      EXPECT_EQ(iFail, 0);
      /*try to get keys form Btree*/
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        iRet = btree.get( pstrKey[ iLoop ], iGet);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(iSuccess, 0);
      EXPECT_EQ(iFail, TEST_COUNT);
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /* test insert random keys to Btree and remove keys by reverse*/
    TEST(KeyBtree_Remove_Char_Test, remove_02_random_Put_down_Remove)
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
        // printf("%d %s\n",iLoop, pstrKey[ iLoop ]);
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
      /*remove keys form Btree*/
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = TEST_COUNT - 1; iLoop > -1; iLoop --)
      {
        iRet = btree.remove( pstrKey[ iLoop ]);
        //  printf("%d %s\n",iLoop, pstrKey[ iLoop ]);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(0, btree.get_object_count() );
      EXPECT_EQ(iSuccess, iSuccess_tmp );
      EXPECT_EQ(iFail, 0);
      /*try to get keys form Btree*/
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        iRet = btree.get( pstrKey[ iLoop ], iGet);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(iSuccess, 0);
      EXPECT_EQ(iFail, TEST_COUNT);
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /* test insert random keys to Btree and remove keys by intersection*/
    TEST(KeyBtree_Remove_Char_Test, remove_03_random_Put_intersect_Remove)
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
      /*remove keys form Btree*/
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0, jLoop = TEST_COUNT - 1; iLoop <= jLoop ; iLoop ++, jLoop--)
      {
        iRet = btree.remove( pstrKey[ iLoop ]);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
        if(iLoop < jLoop)
        {
          iRet = btree.remove( pstrKey[ jLoop ]);
          if(iRet == ERROR_CODE_OK)
          {
            iSuccess ++;
          }
          else
          {
            iFail ++;
          }
        }
      }
      EXPECT_EQ(0, btree.get_object_count() );
      EXPECT_EQ(iSuccess, iSuccess_tmp );
      EXPECT_EQ(iFail, 0);
      /*try to get keys form Btree*/
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        iRet = btree.get( pstrKey[ iLoop ], iGet);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(iSuccess, 0);
      EXPECT_EQ(iFail, TEST_COUNT);
      /*release resources*/
      tear_down();
      btree.clear();
    }

    /* test insert up keys to Btree and remove keys by sequence*/
    TEST(KeyBtree_Remove_Char_Test, remove_04_up_Put_up_Remove)
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
      /*remove keys form Btree*/
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        iRet = btree.remove( pstrKey[ iLoop ]);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(0, btree.get_object_count() );
      EXPECT_EQ(iSuccess, iSuccess_tmp );
      EXPECT_EQ(iFail, 0);
      /*try to get keys form Btree*/
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        iRet = btree.get( pstrKey[ iLoop ], iGet);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(iSuccess, 0);
      EXPECT_EQ(iFail, TEST_COUNT);
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /* test insert up keys to Btree and remove keys by reverse*/
    TEST(KeyBtree_Remove_Char_Test, remove_05_up_Put_down_Remove)
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
      /*remove keys form Btree*/
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = TEST_COUNT - 1; iLoop > -1; iLoop --)
      {
        iRet = btree.remove( pstrKey[ iLoop ]);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(0, btree.get_object_count() );
      EXPECT_EQ(iSuccess, iSuccess_tmp );
      EXPECT_EQ(iFail, 0);
      /*try to get keys form Btree*/
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        iRet = btree.get( pstrKey[ iLoop ], iGet);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(iSuccess, 0);
      EXPECT_EQ(iFail, TEST_COUNT);
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /* test insert up keys to Btree and remove keys by intersection*/
    TEST(KeyBtree_Remove_Char_Test, remove_06_up_Put_intersect_Remove)
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
      /*remove keys form Btree*/
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0, jLoop = TEST_COUNT - 1; iLoop <= jLoop ; iLoop ++, jLoop--)
      {
        iRet = btree.remove( pstrKey[ iLoop ]);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
        if(iLoop < jLoop)
        {
          iRet = btree.remove( pstrKey[ jLoop ]);
          if(iRet == ERROR_CODE_OK)
          {
            iSuccess ++;
          }
          else
          {
            iFail ++;
          }
        }
      }
      EXPECT_EQ(0, btree.get_object_count() );
      EXPECT_EQ(iSuccess, iSuccess_tmp );
      EXPECT_EQ(iFail, 0);
      /*try to get keys form Btree*/
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        iRet = btree.get( pstrKey[ iLoop ], iGet);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(iSuccess, 0);
      EXPECT_EQ(iFail, TEST_COUNT);
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /* test insert down keys to Btree and remove keys by sequence*/
    TEST(KeyBtree_Remove_Char_Test, remove_07_down_Put_up_Remove)
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
      /*remove keys form Btree*/
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        iRet = btree.remove( pstrKey[ iLoop ]);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(0, btree.get_object_count() );
      EXPECT_EQ(iSuccess, iSuccess_tmp );
      EXPECT_EQ(iFail, 0);
      /*try to get keys form Btree*/
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        iRet = btree.get( pstrKey[ iLoop ], iGet);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(iSuccess, 0);
      EXPECT_EQ(iFail, TEST_COUNT);
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /* test insert down keys to Btree and remove keys by reverse*/
    TEST(KeyBtree_Remove_Char_Test, remove_08_down_Put_down_Remove)
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
      /*remove keys form Btree*/
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = TEST_COUNT - 1; iLoop > -1; iLoop --)
      {
        iRet = btree.remove( pstrKey[ iLoop ]);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(0, btree.get_object_count() );
      EXPECT_EQ(iSuccess, iSuccess_tmp );
      EXPECT_EQ(iFail, 0);
      /*try to get keys form Btree*/
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        iRet = btree.get( pstrKey[ iLoop ], iGet);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(iSuccess, 0);
      EXPECT_EQ(iFail, TEST_COUNT);
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /* test insert random keys to Btree and remove keys by intersection*/
    TEST(KeyBtree_Remove_Char_Test, remove_09_down_Put_intersect_Remove)
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
      /*remove keys form Btree*/
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0, jLoop = TEST_COUNT - 1; iLoop <= jLoop ; iLoop ++, jLoop--)
      {
        iRet = btree.remove( pstrKey[ iLoop ]);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
        if(iLoop < jLoop)
        {
          iRet = btree.remove( pstrKey[ jLoop ]);
          if(iRet == ERROR_CODE_OK)
          {
            iSuccess ++;
          }
          else
          {
            iFail ++;
          }
        }
      }
      EXPECT_EQ(0, btree.get_object_count() );
      EXPECT_EQ(iSuccess, iSuccess_tmp );
      EXPECT_EQ(iFail, 0);
      /*try to get keys form Btree*/
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        iRet = btree.get( pstrKey[ iLoop ], iGet);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(iSuccess, 0);
      EXPECT_EQ(iFail, TEST_COUNT);
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /* test insert intersect  keys to Btree and remove keys by sequence*/
    TEST(KeyBtree_Remove_Char_Test, remove_10_intersec_Put_up_Remove)
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
      /*remove keys form Btree*/
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        iRet = btree.remove( pstrKey[ iLoop ]);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(0, btree.get_object_count() );
      EXPECT_EQ(iSuccess, iSuccess_tmp );
      EXPECT_EQ(iFail, 0);
      /*try to get keys form Btree*/
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        iRet = btree.get( pstrKey[ iLoop ], iGet);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(iSuccess, 0);
      EXPECT_EQ(iFail, TEST_COUNT);
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /* test insert intersect keys to Btree and remove keys by reverse*/
    TEST(KeyBtree_Remove_Char_Test, remove_11_intersect_Put_down_Remove)
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
      /*remove keys form Btree*/
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = TEST_COUNT - 1; iLoop > -1; iLoop --)
      {
        iRet = btree.remove( pstrKey[ iLoop ]);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(0, btree.get_object_count() );
      EXPECT_EQ(iSuccess, iSuccess_tmp );
      EXPECT_EQ(iFail, 0);
      /*try to get keys form Btree*/
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        iRet = btree.get( pstrKey[ iLoop ], iGet);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(iSuccess, 0);
      EXPECT_EQ(iFail, TEST_COUNT);
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /* test insert intersect keys to Btree and remove keys by intersection*/
    TEST(KeyBtree_Remove_Char_Test, remove_12_intersect_Put_intersect_Remove)
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
      /*remove keys form Btree*/
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0, jLoop = TEST_COUNT - 1; iLoop <= jLoop ; iLoop ++, jLoop--)
      {
        iRet = btree.remove( pstrKey[ iLoop ]);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
        if(iLoop < jLoop)
        {
          iRet = btree.remove( pstrKey[ jLoop ]);
          if(iRet == ERROR_CODE_OK)
          {
            iSuccess ++;
          }
          else
          {
            iFail ++;
          }
        }
      }
      EXPECT_EQ(0, btree.get_object_count() );
      EXPECT_EQ(iSuccess, iSuccess_tmp );
      EXPECT_EQ(iFail, 0);
      /*try to get keys form Btree*/
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        iRet = btree.get( pstrKey[ iLoop ], iGet);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(iSuccess, 0);
      EXPECT_EQ(iFail, TEST_COUNT);
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /**
     * test remove a key multipy times from Btree
     */
    TEST(KeyBtree_Remove_Char_Test, remove_13_samekey)
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

      /* Remove the key-value */
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        iRet = btree.remove( key );
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
      EXPECT_EQ( iFail, TEST_COUNT - 1 );
      EXPECT_EQ( iSuccess, 1 );

      /* get the value for verify*/
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        iRet = btree.get( key, iGet );
        if (iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ( iFail, TEST_COUNT );
      EXPECT_EQ( iSuccess, 0 );
      free(key);
    }
    /**
     * test remove a key which the Btree don't have
     */
    TEST(KeyBtree_Remove_Char_Test, remove_14_nullkey)
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

      /* Remove the key-value */
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        iRet = btree.remove( key );
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

      /* get the value for verify*/
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        iRet = btree.get( key, iGet );
        if (iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ( iFail, TEST_COUNT );
      EXPECT_EQ( iSuccess, 0 );
      free(key);
    }
    /* test insert long keys to Btree */
    TEST(KeyBtree_Remove_Char_Test, remove_15_longkey)
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
      /*remove keys form Btree*/
      iSuccess_tmp = iSuccess;
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        iRet = btree.remove( pstrKey[ iLoop ]);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(0, btree.get_object_count() );
      EXPECT_EQ(iSuccess, iSuccess_tmp );
      EXPECT_EQ(iFail, 0);
      /*try to get keys form Btree*/
      iSuccess = 0;
      iFail = 0;
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        iRet = btree.get( pstrKey[ iLoop ], iGet);
        if(iRet == ERROR_CODE_OK)
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
      }
      EXPECT_EQ(iSuccess, 0);
      EXPECT_EQ(iFail, TEST_COUNT);
      /*release resources*/
      tear_down();
      btree.clear();
    }
    /**
     * test insert a key and remove a key then insert a key and remove
     * which proceed several times
     */
    TEST(KeyBtree_Remove_Char_Test, remove_16_put_and_remove)
    {
      int32_t iRet = 0;
      int32_t iValue = 0;
      int32_t iSuccess = 0;
      int32_t iFail = 0;
      //int32_t iGet = -1;
      char * key = NULL;
      charBtree btree( MAX_SIZE );

      /* Create the key */
      get_next_random_value(iValue);
      key = (char *)malloc(TEST_SIZE);
      sprintf( key, "%08x", iValue );
      /* insert a key to Btree */
      for (int32_t iLoop = 0; iLoop < TEST_COUNT; iLoop ++)
      {
        /* reset iSuccess and iFail */
        iSuccess = 0;
        iFail = 0;
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
        EXPECT_EQ( iFail, 0 );
        EXPECT_EQ( iSuccess, btree.get_object_count() );
        EXPECT_EQ( iSuccess, 1 );
        /* reset iSuccess and iFail */
        iSuccess = 0;
        iFail = 0;
        /* remove the key*/
        iRet = btree.remove( key );
        if( iRet == ERROR_CODE_OK )
        {
          iSuccess ++;
        }
        else
        {
          iFail ++;
        }
        EXPECT_EQ( iFail, 0);
        EXPECT_EQ( iSuccess , 1);
        EXPECT_EQ( 0, btree.get_object_count() );
      }
      EXPECT_EQ( 0, btree.get_object_count() );
      /* reset iSuccess and iFail */
      iSuccess = 0;
      iFail = 0;
      iRet = btree.put( key, 1 );
      if (iRet == ERROR_CODE_OK)
      {
        iSuccess ++;
      }
      else
      {
        iFail ++;
      }
      EXPECT_EQ( iFail, 0 );
      EXPECT_EQ( iSuccess, btree.get_object_count() );
      EXPECT_EQ( iSuccess, 1 );
      /* release resources */
      free(key);
      btree.clear();
    }
  }//end common
}//end oceanbase
