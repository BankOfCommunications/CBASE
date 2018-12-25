#include <stdio.h>
#include <sys/time.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <stdint.h>
#include <iostream>
#include "utility.h"
#include "common/ob_string_search.h"

#define MAX_PATTERN_LEN 1024
#define MULTIPLE(a) ((a)<<4)+(a) 
#define PRIMER 17

using namespace std;
using namespace oceanbase;
using namespace common;
int primer[150]={2,3,5,7,11,13,17,19,23,29,31,37,41,43,47,
                53,59,61,67,71,73,79,83,89,97,101,103,107,109,113,
                127,131,137,139,149,151,157,163,167,173,179,181,191,193,197,
                199,211,223,227,229,233,239,241,251,257,263,269,271,277,281,
                283,293,307,311,313,317,331,337,347,349,353,359,367,373,379,
                383,389,397,401,409,419,421,431,433,439,443,449,457,461,463,
                467,479,487,491,499,503,509,521,523,541,547,557,563,569,571,
                577,587,593,599,601,607,613,617,619,631,641,643,647,653,659,
                661,673,677,683,691,701,709,719,727,733,739,743,751,757,761,
                769,773,787,797,809,811,821,823,827,829,839,853,857,859,863};

int64_t table[256]={-258047219257472,-256031225357023,-254015231456574,-251999237556125,-249983243655676,-247967249755227,-245951255854778,-243935261954329,-241919268053880,-239903274153431,-237887280252982,-235871286352533,-233855292452084,-231839298551635,-229823304651186,-227807310750737,-225791316850288,-223775322949839,-221759329049390,-219743335148941,-217727341248492,-215711347348043,-213695353447594,-211679359547145,-209663365646696,-207647371746247,-205631377845798,-203615383945349,-201599390044900,-199583396144451,-197567402244002,-195551408343553,-193535414443104,-191519420542655,-189503426642206,-187487432741757,-185471438841308,-183455444940859,-181439451040410,-179423457139961,-177407463239512,-175391469339063,-173375475438614,-171359481538165,-169343487637716,-167327493737267,-165311499836818,-163295505936369,-161279512035920,-159263518135471,-157247524235022,-155231530334573,-153215536434124,-151199542533675,-149183548633226,-147167554732777,-145151560832328,-143135566931879,-141119573031430,-139103579130981,-137087585230532,-135071591330083,-133055597429634,-131039603529185,-129023609628736,-127007615728287,-124991621827838,-122975627927389,-120959634026940,-118943640126491,-116927646226042,-114911652325593,-112895658425144,-110879664524695,-108863670624246,-106847676723797,-104831682823348,-102815688922899,-100799695022450,-98783701122001,-96767707221552,-94751713321103,-92735719420654,-90719725520205,-88703731619756,-86687737719307,-84671743818858,-82655749918409,-80639756017960,-78623762117511,-76607768217062,-74591774316613,-72575780416164,-70559786515715,-68543792615266,-66527798714817,-64511804814368,-62495810913919,-60479817013470,-58463823113021,-56447829212572,-54431835312123,-52415841411674,-50399847511225,-48383853610776,-46367859710327,-44351865809878,-42335871909429,-40319878008980,-38303884108531,-36287890208082,-34271896307633,-32255902407184,-30239908506735,-28223914606286,-26207920705837,-24191926805388,-22175932904939,-20159939004490,-18143945104041,-16127951203592,-14111957303143,-12095963402694,-10079969502245,-8063975601796,-6047981701347,-4031987800898,-2015993900449,0,2015993900449,4031987800898,6047981701347,8063975601796,10079969502245,12095963402694,14111957303143,16127951203592,18143945104041,20159939004490,22175932904939,24191926805388,26207920705837,28223914606286,30239908506735,32255902407184,34271896307633,36287890208082,38303884108531,40319878008980,42335871909429,44351865809878,46367859710327,48383853610776,50399847511225,52415841411674,54431835312123,56447829212572,58463823113021,60479817013470,62495810913919,64511804814368,66527798714817,68543792615266,70559786515715,72575780416164,74591774316613,76607768217062,78623762117511,80639756017960,82655749918409,84671743818858,86687737719307,88703731619756,90719725520205,92735719420654,94751713321103,96767707221552,98783701122001,100799695022450,102815688922899,104831682823348,106847676723797,108863670624246,110879664524695,112895658425144,114911652325593,116927646226042,118943640126491,120959634026940,122975627927389,124991621827838,127007615728287,129023609628736,131039603529185,133055597429634,135071591330083,137087585230532,139103579130981,141119573031430,143135566931879,145151560832328,147167554732777,149183548633226,151199542533675,153215536434124,155231530334573,157247524235022,159263518135471,161279512035920,163295505936369,165311499836818,167327493737267,169343487637716,171359481538165,173375475438614,175391469339063,177407463239512,179423457139961,181439451040410,183455444940859,185471438841308,187487432741757,189503426642206,191519420542655,193535414443104,195551408343553,197567402244002,199583396144451,201599390044900,203615383945349,205631377845798,207647371746247,209663365646696,211679359547145,213695353447594,215711347348043,217727341248492,219743335148941,221759329049390,223775322949839,225791316850288,227807310750737,229823304651186,231839298551635,233855292452084,235871286352533,237887280252982,239903274153431,241919268053880,243935261954329,245951255854778,247967249755227,249983243655676,251999237556125,254015231456574,256031225357023};

static const int num = 4;

int time_elaspe(struct timeval* start, struct timeval* end)  
{  
  return static_cast<int32_t>(1000000*(end->tv_sec - start->tv_sec) + end->tv_usec - start->tv_usec);  
}   


void cal_next(const char* t, int32_t next[], int32_t len)
{
  int32_t i,j;
  next[0] = -1; //Attention: next[0] equals -1 other than 0
  i = 0;
  j = -1;
  while(i < len)
  {
    while(j > -1 && t[j] != t[i])
      j = next[j];
    
    i++, j++;
    if(t[j] == t[i]) 
      next[i] = next[j];
    else
      next[i] = j;
  }
}

// KMP, ensure strlen(t) < MAX_PATTERN_LEN
int32_t KMP(const char* s,int32_t lens, const char* t, int32_t lent)
{
  int32_t next[MAX_PATTERN_LEN];
  //int lens = strlen(s);
  //int lent = strlen(t);
  cal_next(t, next, lent);
  int32_t i,j;
  i = j = 0;
  while(i < lens)
  {
    while(j > -1 && t[j] != s[i])
      j = next[j];
    i++, j++;
    if(j >= lent) return i - j;
  }
  return -1;
}

int32_t fastsearch(const char* s, int32_t n, const char* p, int32_t m)
{
  long mask;
  int32_t skip = 0;
  int32_t i, j, mlast, w;

  w = n - m;

  if (w < 0)
    return -1;

  /* look for special cases */
  if (m <= 1) {
    if (m <= 0)
      return -1;
    /* use special case for 1-character strings */
    for (i = 0; i < n; i++)
      if (s[i] == p[0])
        return i;
    return -1;
  }

  mlast = m - 1;

  /* create compressed boyer-moore delta 1 table */
  skip = mlast - 1;
  /* process pattern[:-1] */
  for (mask = i = 0; i < mlast; i++) {
    mask |= (1 << (p[i] & 0x1F));
    if (p[i] == p[mlast])
      skip = mlast - i - 1;
  }
  /* process pattern[-1] outside the loop */
  mask |= (1 << (p[mlast] & 0x1F));

  for (i = 0; i <= w; i++) {
    /* note: using mlast in the skip path slows things down on x86 */
    if (s[i+m-1] == p[m-1]) {
      /* candidate match */
      for (j = 0; j < mlast; j++)
        if (s[i+j] != p[j])
          break;
      if (j == mlast) {
        /* got a match! */
        return i;
      }
      /* miss: check if next character is part of pattern */
      if (!(mask & (1 << (s[i+m] & 0x1F))))
        i = i + m;
      else
        i = i + skip;
    } else {
      /* skip: check if next character is part of pattern */
      if (!(mask & (1 << (s[i+m] & 0x1F))))
        i = i + m;
    }
  }
  return -1;
}

int32_t KR_search1(const char * pattern, int32_t pattern_len, const char* text, int32_t text_len)
{
  int32_t ret = -1;
  int32_t shift = 1;
  int32_t pat_print = 0;
  int32_t text_print = 0;

  for(int32_t i = 0; i<pattern_len; ++i)
  {
    text_print = text_print * PRIMER + text[i];
    pat_print  = pat_print * PRIMER  + pattern[i];
    if ( 1<= i)
      shift = shift*PRIMER;
  }
 
  int32_t pos = 0;

  while(pos <= text_len - pattern_len)
  {
    if(pat_print == text_print && 0 == memcmp(pattern, text+pos, pattern_len))
    {
      ret = pos;
      break;
    }
    text_print = (text_print - text[pos]*shift)*PRIMER + text[pos+pattern_len];
    ++pos;
  }
  return ret;
}

int32_t KR_search2(const char * pattern, int32_t pattern_len, const char* text, int32_t text_len)
{
  int32_t ret = -1;
  int32_t shift = 1;
  int32_t pat_print = 0;
  int32_t text_print = 0;

  for(int32_t i = 0; i<pattern_len; ++i)
  {
    text_print = text_print * PRIMER + text[i];
    pat_print  = pat_print * PRIMER  + pattern[i];
    shift = shift * PRIMER;
  }
 
  int32_t pos = 0;

  while(pos <= text_len - pattern_len)
  {
    if(pat_print == text_print && 0 == memcmp(pattern, text+pos, pattern_len))
    {
      ret = pos;
      break;
    }
    text_print = text_print*PRIMER - text[pos]*shift + text[pos+pattern_len];
    ++pos;
  }
  return ret;
}


int32_t KR_search3(const char * pattern, int32_t pattern_len, const char* text, int32_t text_len)
{
  int32_t ret = -1;
  int32_t pat_print = 0;
  int32_t text_print = 0;


  for(int32_t i = 0; i<pattern_len; ++i)
  {
    text_print = text_print * PRIMER + text[i];
    pat_print  = pat_print * PRIMER  + pattern[i];
  }
 
  int32_t pos = 0;

  while(pos <= text_len - pattern_len)
  {
    if(pat_print == text_print && 0 == memcmp(pattern, text+pos, pattern_len))
    {
      ret = pos;
      break;
    }
    text_print = static_cast<int32_t>(text_print*PRIMER - table[text[pos]+128] + text[pos+pattern_len]);
    ++pos;
  }
  return ret;
}

int32_t KR_search4(const char * pattern, int32_t pattern_len, const char* text, int32_t text_len)
{
  int32_t ret = -1;
  int32_t pat_print = 0;
  int32_t text_print = 0;

  for(int32_t i = 0; i<pattern_len; ++i)
  {
    text_print = (text_print << num) + text_print + text[i];
    pat_print  = (pat_print << num) + pat_print + pattern[i];
  }
 
  int32_t pos = 0;

  while(pos <= text_len - pattern_len)
  {
    if(pat_print == text_print && 0 == memcmp(pattern, text+pos, pattern_len))
    {
      ret = pos;
      break;
    }
    text_print = static_cast<int32_t>((text_print << num) + text_print - table[text[pos]+128] + text[pos+pattern_len]);
    ++pos;
  }
  return ret;
}


int32_t KR_search5(const char * pattern, int32_t pattern_len, uint64_t pat_print, const char* text, int32_t text_len, int64_t* table)
{
  int32_t ret = -1;
  uint64_t text_print = 0;

  for(int32_t i = 0; i<pattern_len; ++i)
  {
    text_print = (text_print << num) + text_print + text[i];
  }
 
  int32_t pos = 0;

  while(pos <= text_len - pattern_len)
  {
    if(pat_print == text_print && 0 == memcmp(pattern, text+pos, pattern_len))
    {
      ret = pos;
      break;
    }
    text_print = (text_print << num) + text_print -  /*text[pos]*shift*/table[text[pos]+128] + text[pos+pattern_len];
    ++pos;
  }
  return ret;
}

void cal_table(int pattern_len, int64_t* table)
{
  uint64_t shift = 1;
  for(int i = 0; i < pattern_len; i++)
  {
    shift = (shift << num) + shift;
  }
  
  for(int i = -128 ; i < 128 ; ++i)
  {
    table[i+128] = i*shift;
  }
}

uint64_t cal_pattern_finger_print(const char * pattern, int len)
{
  uint64_t pat_print = 0;
  for( int i = 0; i < len; ++i)
  {
      pat_print = (pat_print << num) + pat_print + pattern[i];
  }
  return pat_print;
}

int main(int argc, char ** argv)
{
  UNUSED(argc);
  UNUSED(argv);
  struct timeval start, end;
  const char *pattern[8]={"Free softw","d be free "," socially ","bjects—su"," that it c","y. These p","abcdefghigklmnopqrstuvwxyzABCDEFGIJKLMNOPQRSTUVWXYZ", "hellohello"};
  
  const char *text    = "Free software is a matter of freedom: people should be free to use software in all the ways that are socially useful. Software differs from material objects—such as chairs, sandwiches, and gasoline—in that it can be copied and changed much more easily. These possibilities make software as useful as abcdefghigklmnopqrstuvwxyzABCDEFGIJKLMNOPQRSTUVWXYZ it is; we believe software use";
 
  typedef int32_t (*search_func)(const char *, int32_t, const char*, int32_t);
  search_func funcs[5];
  funcs[0]=KR_search1;
  funcs[1]=KR_search2;
  funcs[2]=KR_search3;
  funcs[3]=KR_search4;
//  funcs[4]=KR_search5;

 
  int32_t t_length = static_cast<int32_t>(strlen(text));
  for ( int i=0 ;i<8;i++)
  {
    ObString text_str(0, t_length, (char*)text);
    int32_t p_length = static_cast<int32_t>(strlen(pattern[i]));
    uint64_t pat_print = cal_pattern_finger_print(pattern[i],p_length);
    int64_t table[256];
    ObString pattern_str;
    pattern_str.assign((char*)pattern[i], p_length);
    cal_table(p_length, table);
    printf("\n");
    printf("fast_search find match position is %d\n", fastsearch(text, t_length, pattern[i], p_length));
    printf("KMP_search find match position is %d\n", KMP(text, t_length, pattern[i], p_length));
    // printf("KR_search  find match position is %d\n", KR_search1(pattern[i], p_length, text, t_length));
    // printf("KR_search2 find match position is %d\n", KR_search2(pattern[i], p_length, text, t_length));
    //printf("KR_search3 find match position is %d\n", KR_search3(pattern[i], p_length, text, t_length));
    //printf("KR_search4 find match position is %d\n", KR_search4(pattern[i], p_length, text, t_length));
    printf("KR_search5 find match position is %d\n", KR_search5(pattern[i], p_length,  pat_print, text, t_length, table));
    printf("kr_search find match position is %ld\n", ObStringSearch::kr_search(pattern_str, text_str));
   
    
    gettimeofday(&start,0);
    for(int time = 0; time < 1000000 ; ++time)
    {
      ObStringSearch::fast_search(text_str, pattern_str);
    }
    gettimeofday(&end,0);
    printf("fast_search\t%d\n",time_elaspe(&start,&end));
    

    gettimeofday(&start,0);
    for(int time = 0; time < 1000000 ; ++time)
    {
      KMP(text, t_length, pattern[i], p_length);
    }
    gettimeofday(&end,0);
    printf("KMP2_search\t%d\n",time_elaspe(&start,&end));

    /*for(int index=0; index<4; ++index)
    {
      gettimeofday(&start,0);
      for(int time = 0; time < 1000000 ; ++time)
      {
        (*funcs[index])(pattern[i], p_length, text, t_length);
      }
      gettimeofday(&end,0);
      printf("KR_search%d\t%d\n",index+1,time_elaspe(&start, &end));
      }*/

    gettimeofday(&start,0);
    for(int time = 0; time < 1000000 ; ++time)
    {
      KR_search5(pattern[i], p_length, pat_print, text, t_length, table);
    }
    gettimeofday(&end,0);
    printf("KR_search\t%d\n",time_elaspe(&start, &end));

    gettimeofday(&start,0);
    for(int time = 0; time < 1000000 ; ++time)
    {
      ObStringSearch::kr_search(pattern_str, text_str);
    }
    gettimeofday(&end,0);
    printf("kr_search\t%d\n",time_elaspe(&start, &end));
  }
}
