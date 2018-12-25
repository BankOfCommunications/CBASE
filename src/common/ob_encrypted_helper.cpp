/* (C) 2010-2012 Alibaba Group Holding Limited. 
 * 
 * This program is free software: you can redistribute it and/or 
 *  modify it under the terms of the GNU General Public License 
 *  version 2 as published by the Free Software Foundation.
 * 
 * Version: 0.1 
 * 
 * Authors: 
 *    Wu Di <lide.wd@alipay.com>
 */
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include "ob_encrypted_helper.h"

using namespace oceanbase;
using namespace oceanbase::common;

const uint32_t ObEncryptedHelper::sha_const_key[5] = 
{
  0x67452301,
  0xEFCDAB89,
  0x98BADCFE,
  0x10325476,
  0xC3D2E1F0
};
const uint32_t ObEncryptedHelper::K[4] =
{
  0x5A827999,
  0x6ED9EBA1,
  0x8F1BBCDC,
  0xCA62C1D6
};

/**
 * @synopsis  encrypt_from_scrambled 将不可见字符转成可见字符
 *
 * @param encrypted_pass 外部分配好了内存,大小等于SCRAMBLE_LENGTH * 2 + 1
 * @param scrambled_password  SCRAMBLE_LENGTH 个字节
 */
void ObEncryptedHelper::encrypt_from_scrambled(ObString &encrypted_pass, const ObString &scrambled_password)
{
  char *scrambled2 = encrypted_pass.ptr();
  int i = 0;
  int cnt = 0;
  for (i = 0;i < SCRAMBLE_LENGTH;++i)
  {
    cnt = snprintf(scrambled2 + i * 2, 3, "%02hhx", (scrambled_password.ptr())[i]);
    OB_ASSERT(cnt == 2);
  }
}

/**
 * @synopsis  encrypt  对明文进行加密
 *
 * @param encrypted_pass 外部分配好了内存,大小等于SCRAMBLE_LENGTH * 2 + 1
 * @param password       明文密码
 */
void ObEncryptedHelper::encrypt(ObString &encrypted_pass, const ObString &password)
{
  char scrambled[SCRAMBLE_LENGTH + 1];
  memset(scrambled, 0, sizeof(scrambled));
  scramble(scrambled, "aaaaaaaabbbbbbbbbbbb", password);
  char *scrambled2 = encrypted_pass.ptr();
  int i = 0;
  int cnt = 0;
  // 把不可见字符转成十六进制
  for (i = 0;i < SCRAMBLE_LENGTH;++i)
  {
    cnt = snprintf(scrambled2 + i * 2, 3, "%02hhx", scrambled[i]);
    OB_ASSERT(cnt == 2);
  }
}

/*
    Produce an obscure octet sequence from password and random
    string, recieved from the server. This sequence corresponds to the
    password, but password can not be easily restored from it. The sequence
    is then sent to the server for validation. Trailing zero is not stored
    in the buf as it is not needed.
    This function is used by client to create authenticated reply to the
    server's greeting.
  SYNOPSIS
    scramble()
    buf       OUT store scrambled string here. The buf must be at least 
                  SHA1_HASH_SIZE bytes long. 
    message   IN  random message, must be exactly SCRAMBLE_LENGTH long and 
                  NULL-terminated.
    password  IN  users' password 
*/
void ObEncryptedHelper::scramble(char *to, const char *message, const ObString &password)
{
  SHA1_CONTEXT sha1_context;
  unsigned char hash_stage1[SHA1_HASH_SIZE];
  unsigned char hash_stage2[SHA1_HASH_SIZE];

  mysql_sha1_reset(&sha1_context);
  /* stage 1: hash password */
  mysql_sha1_input(&sha1_context, (unsigned char *) password.ptr(), (uint32_t) password.length());
  mysql_sha1_result(&sha1_context, hash_stage1);
  /* stage 2: hash stage 1; note that hash_stage2 is stored in the database */
  mysql_sha1_reset(&sha1_context);
  mysql_sha1_input(&sha1_context, hash_stage1, SHA1_HASH_SIZE);
  mysql_sha1_result(&sha1_context, hash_stage2);
  /* create crypt string as sha1(message, hash_stage2) */;
  mysql_sha1_reset(&sha1_context);
  mysql_sha1_input(&sha1_context, (const unsigned char *) message, SCRAMBLE_LENGTH);
  mysql_sha1_input(&sha1_context, hash_stage2, SHA1_HASH_SIZE);
  /* xor allows 'from' and 'to' overlap: lets take advantage of it */
  mysql_sha1_result(&sha1_context, (unsigned char *) to);
  my_crypt(to, (const unsigned char *) to, hash_stage1, SCRAMBLE_LENGTH);
}
/*
  Process the next 512 bits of the message stored in the Message_Block array.

  SYNOPSIS
    SHA1ProcessMessageBlock()

   DESCRIPTION
     Many of the variable names in this code, especially the single
     character names, were used because those were the names used in
     the publication.
*/
void ObEncryptedHelper::SHA1ProcessMessageBlock(SHA1_CONTEXT *context)
{
  int		t = 0;		   /* Loop counter		  */
  uint32_t	temp = 0;		   /* Temporary word value	  */
  uint32_t	W[80];		   /* Word sequence		  */
  uint32_t	A = 0, B = 0, C = 0, D = 0, E = 0;	   /* Word buffers		  */
  int idx = 0;

  /*
    Initialize the first 16 words in the array W
  */

  for (t = 0; t < 16; t++)
  {
    idx=t*4;
    W[t] = context->Message_Block[idx] << 24;
    W[t] |= context->Message_Block[idx + 1] << 16;
    W[t] |= context->Message_Block[idx + 2] << 8;
    W[t] |= context->Message_Block[idx + 3];
  }


  for (t = 16; t < 80; t++)
  {
    W[t] = SHA1CircularShift(1,W[t-3] ^ W[t-8] ^ W[t-14] ^ W[t-16]);
  }

  A = context->Intermediate_Hash[0];
  B = context->Intermediate_Hash[1];
  C = context->Intermediate_Hash[2];
  D = context->Intermediate_Hash[3];
  E = context->Intermediate_Hash[4];

  for (t = 0; t < 20; t++)
  {
    temp= SHA1CircularShift(5,A) + ((B & C) | ((~B) & D)) + E + W[t] + K[0];
    E = D;
    D = C;
    C = SHA1CircularShift(30,B);
    B = A;
    A = temp;
  }

  for (t = 20; t < 40; t++)
  {
    temp = SHA1CircularShift(5,A) + (B ^ C ^ D) + E + W[t] + K[1];
    E = D;
    D = C;
    C = SHA1CircularShift(30,B);
    B = A;
    A = temp;
  }

  for (t = 40; t < 60; t++)
  {
    temp= (SHA1CircularShift(5,A) + ((B & C) | (B & D) | (C & D)) + E + W[t] +
     K[2]);
    E = D;
    D = C;
    C = SHA1CircularShift(30,B);
    B = A;
    A = temp;
  }

  for (t = 60; t < 80; t++)
  {
    temp = SHA1CircularShift(5,A) + (B ^ C ^ D) + E + W[t] + K[3];
    E = D;
    D = C;
    C = SHA1CircularShift(30,B);
    B = A;
    A = temp;
  }

  context->Intermediate_Hash[0] += A;
  context->Intermediate_Hash[1] += B;
  context->Intermediate_Hash[2] += C;
  context->Intermediate_Hash[3] += D;
  context->Intermediate_Hash[4] += E;

  context->Message_Block_Index = 0;
}

/*
  Pad message

  SYNOPSIS
    SHA1PadMessage()
    context: [in/out]		The context to pad

  DESCRIPTION
    According to the standard, the message must be padded to an even
    512 bits.  The first padding bit must be a '1'. The last 64 bits
    represent the length of the original message.  All bits in between
    should be 0.  This function will pad the message according to
    those rules by filling the Message_Block array accordingly.  It
    will also call the ProcessMessageBlock function provided
    appropriately. When it returns, it can be assumed that the message
    digest has been computed.

*/
void ObEncryptedHelper::SHA1PadMessage(SHA1_CONTEXT *context)
{
  /*
    Check to see if the current message block is too small to hold
    the initial padding bits and length.  If so, we will pad the
    block, process it, and then continue padding into a second
    block.
  */

  int i=context->Message_Block_Index;

  if (i > 55)
  {
    context->Message_Block[i++] = 0x80;
    memset((char*) &context->Message_Block[i],0,sizeof(context->Message_Block[0])*(64-i));
    context->Message_Block_Index=64;

    /* This function sets context->Message_Block_Index to zero	*/
    SHA1ProcessMessageBlock(context);

    memset((char*) &context->Message_Block[0],0,sizeof(context->Message_Block[0])*56);
    context->Message_Block_Index=56;
  }
  else
  {
    context->Message_Block[i++] = 0x80;
    memset((char*) &context->Message_Block[i],0,sizeof(context->Message_Block[0])*(56-i));
    context->Message_Block_Index=56;
  }

  /*
    Store the message length as the last 8 octets
  */

  context->Message_Block[56] = (int8_t) (context->Length >> 56);
  context->Message_Block[57] = (int8_t) (context->Length >> 48);
  context->Message_Block[58] = (int8_t) (context->Length >> 40);
  context->Message_Block[59] = (int8_t) (context->Length >> 32);
  context->Message_Block[60] = (int8_t) (context->Length >> 24);
  context->Message_Block[61] = (int8_t) (context->Length >> 16);
  context->Message_Block[62] = (int8_t) (context->Length >> 8);
  context->Message_Block[63] = (int8_t) (context->Length);

  SHA1ProcessMessageBlock(context);
}
/*
  Initialize SHA1Context

  SYNOPSIS
    mysql_sha1_reset()
    context [in/out]		The context to reset.

 DESCRIPTION
   This function will initialize the SHA1Context in preparation
   for computing a new SHA1 message digest.

 RETURN
   SHA_SUCCESS		ok
   != SHA_SUCCESS	sha Error Code.
*/
int ObEncryptedHelper::mysql_sha1_reset(SHA1_CONTEXT *context)
{
  context->Length		  = 0;
  context->Message_Block_Index	  = 0;

  context->Intermediate_Hash[0]   = sha_const_key[0];
  context->Intermediate_Hash[1]   = sha_const_key[1];
  context->Intermediate_Hash[2]   = sha_const_key[2];
  context->Intermediate_Hash[3]   = sha_const_key[3];
  context->Intermediate_Hash[4]   = sha_const_key[4];

  context->Computed   = 0;
  context->Corrupted  = 0;

  return SHA_SUCCESS;
}


/*
   Return the 160-bit message digest into the array provided by the caller

  SYNOPSIS
    mysql_sha1_result()
    context [in/out]		The context to use to calculate the SHA-1 hash.
    Message_Digest: [out]	Where the digest is returned.

  DESCRIPTION
    NOTE: The first octet of hash is stored in the 0th element,
    the last octet of hash in the 19th element.

 RETURN
   SHA_SUCCESS		ok
   != SHA_SUCCESS	sha Error Code.
*/

int ObEncryptedHelper::mysql_sha1_result(SHA1_CONTEXT *context,
                      unsigned char Message_Digest[SHA1_HASH_SIZE])
{
  int i = 0;
  if (!context->Computed)
  {
    SHA1PadMessage(context);
     /* message may be sensitive, clear it out */
    memset((char*) context->Message_Block,0,64);
    context->Length   = 0;    /* and clear length  */
    context->Computed = 1;
  }

  for (i = 0; i < SHA1_HASH_SIZE; i++)
    Message_Digest[i] = (int8_t)((context->Intermediate_Hash[i>>2] >> 8
       * ( 3 - ( i & 0x03 ) )));
  return SHA_SUCCESS;
}


/*
  Accepts an array of octets as the next portion of the message.

  SYNOPSIS
   mysql_sha1_input()
   context [in/out]	The SHA context to update
   message_array	An array of characters representing the next portion
      of the message.
  length		The length of the message in message_array

 RETURN
   SHA_SUCCESS		ok
   != SHA_SUCCESS	sha Error Code.
*/

int ObEncryptedHelper::mysql_sha1_input(SHA1_CONTEXT *context, const unsigned char *message_array,
                     unsigned length)
{
  if (!length)
    return SHA_SUCCESS;
  /* We assume client konows what it is doing in non-debug mode */
  if (!context || !message_array)
    return SHA_NULL;
  if (context->Computed)
    return (context->Corrupted= SHA_STATE_ERROR);
  if (context->Corrupted)
    return context->Corrupted;
  while (length--)
  {
    context->Message_Block[context->Message_Block_Index++]=
      (*message_array & 0xFF);
    context->Length  += 8;  /* Length is in bits */
    /*
      Then we're not debugging we assume we never will get message longer
      2^64 bits.
    */
    if (context->Length == 0)
      return (context->Corrupted= 1);	   /* Message is too long */

    if (context->Message_Block_Index == 64)
    {
      SHA1ProcessMessageBlock(context);
    }
    message_array++;
  }
  return SHA_SUCCESS;
}

/*
    Encrypt/Decrypt function used for password encryption in authentication.
    Simple XOR is used here but it is OK as we crypt random strings. Note,
    that XOR(s1, XOR(s1, s2)) == s2, XOR(s1, s2) == XOR(s2, s1)
  SYNOPSIS
    my_crypt()
    to      OUT buffer to hold crypted string; must be at least len bytes
                long; to and s1 (or s2) may be the same.
    s1, s2  IN  input strings (of equal length)
    len     IN  length of s1 and s2
*/
void ObEncryptedHelper::my_crypt(char *to, const unsigned char *s1, const unsigned char *s2, uint32_t len)
{
  const unsigned char *s1_end= s1 + len;
  while (s1 < s1_end)
    *to++= *s1++ ^ *s2++;
}
