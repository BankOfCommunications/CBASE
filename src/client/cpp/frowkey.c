#include <stdlib.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>
/*********************
 * format row key according to format string
 * example: "D8D1S8" means a 17 byte length row key
 *   d, D mean little endian and big endian of INT value respectively
 *   s, S mean little endian and big endian of VARCHAR value respectively
 *   Z means 0x00 value
 *   F means 0xFF value
 *   numbers mean the length of bytes
 * 
 * frowkey(str, size, "D8D1S8", 31415, 1, "lily")
 * the formated rowkey is "0000000000007AB701000000006C696C79"
 *       0000000000007AB7 01 000000006C696C79
 *       ---------------- -- ----------------
 *            8bytes    1byte    8bytes
 * 
 * frowkey(str, size, "d8D1S8", 31415, 1, "lily")
 * the formated rowkey is "B77A00000000000001000000006C696C79"
 *       B77A000000000000 01 000000006C696C79
 *       ---------------- -- ----------------
 *            8bytes    1byte    8bytes
 * 
 * frowkey(str, size, "d8D1s8", 31415, 1, "lily")
 * the formated rowkey is "B77A000000000000016C696C7900000000"
 *       B77A000000000000 01 6C696C7900000000
 *       ---------------- -- ----------------
 *            8bytes    1byte    8bytes
 * 
 * frowkey(str, size, "D1D8F4Z4", 0, 1234567)
 * the formated rowkey is "00000000000012D687FFFFFFFF00000000"
 *       00 000000000012D687 FFFFFFFF 00000000
 *       -- ---------------- -------- --------
 *     1byte    8bytes       4bytes   4bytes
 */
int frowkey(char* str, size_t size, const char *format, ...)
{
  char*   str_end = str + size;
  char*   cur     = str;
  char    type    = 0;
  int     len     = 0;
  va_list ap;
  va_start(ap, format);
  while (cur < str_end)
  {
    if (0 != type)
    {
      if ('0' <= *format && *format <= '9')
      {
        len *= 10;
        len += *format - '0';
        format++;
        continue;
      }
      else
      {
        if (len <= 0 || cur + len > str_end) goto frowkey_err;
        switch (type)
        {
          case 'D': case 'd':
          {
            uint64_t v = va_arg(ap, uint64_t);
            int counter = 0;
            while (v > 0 && counter < len)
            {
              int b = v % 256;
              v = v / 256;
              cur[counter++] = (char)b;
            }
            if ('D' == type)
            {
              int hlen = len / 2;
              int i = 0;
              for (; i < hlen; i++)
              {
                char r = len - i - 1;
                char t = cur[i];
                cur[i] = cur[r];
                cur[r] = t;
              }
            }
            break;
          }
          case 'S': case 's':
          {
            char* v = va_arg(ap, char*);
            int counter = 0;
            while (*v && counter < len) cur[counter++] = *v++;
            if ('S' == type)
            {
              memmove(cur + len - counter, cur, counter);
              memset(cur, 0x00, len - counter);
            }
            break;
          }
          case 'Z': case 'F':
          {
            char v = type == 'Z' ? 0 : 255;
            int counter = 0;
            while (counter < len) cur[counter++] = v;
            break;
          }
          default:
            goto frowkey_err;
        }
        cur += len;
        type = 0;
        len = 0;
      }
    }
    if (*format == '\0') break;
    switch (*format)
    {
      case 'S': case 's': case 'D': case 'd': case 'Z': case 'F':
        type = *format;
        break;
      default:
        goto frowkey_err;
    }
    format++;
  }
  if (*format != '\0') goto frowkey_err;

  goto frowkey_end;

frowkey_err:
  va_end(ap);
  return -1;

frowkey_end:
  va_end(ap);
  return cur - str;
}

void test_frowkey()
{
  struct
  {
    char* format;
    char* result;
  } correct_cases[] = {
    {"D8D1S18", "0000000000007AB70100000000000000000000000000006C696C79"},
    {"d8D1S18", "B77A0000000000000100000000000000000000000000006C696C79"},
    {"d8D1s18", "B77A000000000000016C696C790000000000000000000000000000"},
    {"d8D1s8F10", "B77A000000000000016C696C7900000000FFFFFFFFFFFFFFFFFFFF"},
    {"d8D1s8Z10", "B77A000000000000016C696C790000000000000000000000000000"},
    {"F5d8D1s8Z5", "FFFFFFFFFFB77A000000000000016C696C79000000000000000000"},
    {"d2D1s2", "B77A016C69"},
    {"D2D1S2", "7AB7016C69"},
    {"d1D1s2", "B7016C69"},
    {"D1D1S1", "B7016C"},
    {NULL, NULL},
  };
  char str[BUFSIZ];
  char buf[BUFSIZ];
  int i = 0;
  int j = 0;
  for (; correct_cases[i].format != NULL; i++)
  {
    memset(str, 0x00, sizeof(str));
    int len = frowkey(str, BUFSIZ, correct_cases[i].format, 31415, 1, "lily");
    fprintf(stderr, "len=%d\n", len);
    assert(len == strlen(correct_cases[i].result) / 2);
    for (j = 0; j < len; j++)
    {
      assert(snprintf(buf + j * 2, 3, "%02X", (unsigned char)str[j]) == 2);
    }
    //printf("%s\n", buf);
    //printf("%s\n", correct_cases[i].result);
    assert(strcmp(buf, correct_cases[i].result) == 0);
  }

  assert(frowkey(buf, 30, "d2D1s5F-14", 31415, 1, "lily") == -1);
  assert(frowkey(buf, 30, "d20D1s5F6", 31415, 1, "lily") == -1);
  assert(frowkey(buf, 30, "dD1s5F6", 31415, 1, "lily") == -1);
  assert(frowkey(buf, 30, "d2D1s5F", 31415, 1, "lily") == -1);
}

int main()
{
  test_frowkey();
  return 0;
}

