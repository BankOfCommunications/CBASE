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

#ifndef OB_ENCRYPTED_HELPER_
#define OB_ENCRYPTED_HELPER_
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include "ob_string.h"

namespace oceanbase
{
  namespace common
  {
    class ObEncryptedHelper
    {
#define SCRAMBLE_LENGTH 20
#define SHA1_HASH_SIZE 20 /* Hash size in bytes */
/*
  Define the SHA1 circular left shift macro
*/

#define SHA1CircularShift(bits,word) \
		(((word) << (bits)) | ((word) >> (32-(bits))))
public:
  //static void encrypt_from_scrambled(ObString &encrypted_pass, const char *password);
  static void encrypt_from_scrambled(ObString &encrypted_pass, const ObString &password);
  static void encrypt(ObString &encrypted_pass, const ObString &password);
private:
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
  static void scramble(char *to, const char *message, const ObString &password);
  const static uint32_t sha_const_key[5];
  enum sha_result_codes
  {
    SHA_SUCCESS = 0,
    SHA_NULL,		/* Null pointer parameter */
    SHA_INPUT_TOO_LONG,	/* input data too long */
    SHA_STATE_ERROR	/* called Input after Result */
  };

  /*
    This structure will hold context information for the SHA-1
    hashing operation
  */

  typedef struct SHA1_CONTEXT
  {
    unsigned long  Length;		/* Message length in bits      */
    uint32_t Intermediate_Hash[SHA1_HASH_SIZE/4]; /* Message Digest  */
    int Computed;			/* Is the digest computed?	   */
    int Corrupted;		/* Is the message digest corrupted? */
    int16_t Message_Block_Index;	/* Index into message block array   */
    uint8_t Message_Block[64];	/* 512-bit message blocks      */
  } SHA1_CONTEXT;
  /*
    Process the next 512 bits of the message stored in the Message_Block array.

    SYNOPSIS
      SHA1ProcessMessageBlock()

     DESCRIPTION
       Many of the variable names in this code, especially the single
       character names, were used because those were the names used in
       the publication.
  */

  /* Constants defined in SHA-1	*/
  static const uint32_t K[4];
  static void SHA1ProcessMessageBlock(SHA1_CONTEXT *context);
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
  static void SHA1PadMessage(SHA1_CONTEXT *context);
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
  static int mysql_sha1_reset(SHA1_CONTEXT *context);

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

  static int mysql_sha1_result(SHA1_CONTEXT *context,unsigned char Message_Digest[SHA1_HASH_SIZE]);

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

  static int mysql_sha1_input(SHA1_CONTEXT *context, const unsigned char *message_array,unsigned length);

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
  static void my_crypt(char *to, const unsigned char *s1, const unsigned char *s2, uint32_t len);
    };
  }// namespace common
}//namespace oceanbase
#endif
