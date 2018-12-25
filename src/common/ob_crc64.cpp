/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *
 * ob_crc64.cpp is for what ... 
 *  
 *   The method to compute the CRC64 is referred to as
 *      CRC-64-ISO:
 *  http://en.wikipedia.org/wiki/Cyclic_redundancy_check The
 *      generator polynomial is x^64 + x^4 + x^3 + x + 1.
 *      Reverse polynom: 0xd800000000000000ULL *
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *   yubai <yubai.lk@taobao.com>
 *
 */
#include <stdlib.h>  
#include "ob_crc64.h"  

namespace oceanbase
{
  namespace common
  {

    /******************************************************************************* 
     *   Global Variables                                                           * 
     *******************************************************************************/ 
    /** 
      * Lookup table (precomputed CRC64 values for each 8 bit string) computation 
      * takes into account the fact that the reverse polynom has zeros in lower 8 bits: 
      * 
      * @code 
      *    for (i = 0; i < 256; i++) 
      *    { 
      *        shiftRegister = i; 
      *        for (j = 0; j < 8; j++) 
      *        { 
      *            if (shiftRegister & 1) 
      *                shiftRegister = (shiftRegister >> 1) ^ Reverse_polynom; 
      *            else 
      *                shiftRegister >>= 1; 
      *        } 
      *        CRCTable[i] = shiftRegister; 
      *    } 
      * @endcode 
      * 
      */ 
    static const uint64_t CRC64_TABLE_SIZE = 256;
    static uint64_t s_crc64_table[CRC64_TABLE_SIZE] = {0};
    static uint16_t s_optimized_crc64_table[CRC64_TABLE_SIZE] = {0};
    
    void __attribute__((constructor)) ob_global_init_crc64_table()
    {
      ob_init_crc64_table(OB_DEFAULT_CRC64_POLYNOM);
    }

    void ob_init_crc64_table(const uint64_t polynom)
    {
      for (uint64_t i = 0; i < CRC64_TABLE_SIZE; i++)
      {
        uint64_t shift = i;
        for (uint64_t j = 0; j < 8; j++)
        {
          if (shift & 1)
          {
            shift = (shift >> 1) ^ polynom;
          }
          else
          {
            shift >>= 1;
          }
        }
        s_crc64_table[i] = shift;
        s_optimized_crc64_table[i] = static_cast<int16_t >((shift >> 48) & 0xffff);
      }
    }
    
    /*
    static const uint64_t s_crc64_table[] = 
    { 
        0x0000000000000000ULL, 0x01B0000000000000ULL, 0x0360000000000000ULL, 0x02D0000000000000ULL, 
        0x06C0000000000000ULL, 0x0770000000000000ULL, 0x05A0000000000000ULL, 0x0410000000000000ULL, 
        0x0D80000000000000ULL, 0x0C30000000000000ULL, 0x0EE0000000000000ULL, 0x0F50000000000000ULL, 
        0x0B40000000000000ULL, 0x0AF0000000000000ULL, 0x0820000000000000ULL, 0x0990000000000000ULL, 
        0x1B00000000000000ULL, 0x1AB0000000000000ULL, 0x1860000000000000ULL, 0x19D0000000000000ULL, 
        0x1DC0000000000000ULL, 0x1C70000000000000ULL, 0x1EA0000000000000ULL, 0x1F10000000000000ULL, 
        0x1680000000000000ULL, 0x1730000000000000ULL, 0x15E0000000000000ULL, 0x1450000000000000ULL, 
        0x1040000000000000ULL, 0x11F0000000000000ULL, 0x1320000000000000ULL, 0x1290000000000000ULL, 
        0x3600000000000000ULL, 0x37B0000000000000ULL, 0x3560000000000000ULL, 0x34D0000000000000ULL, 
        0x30C0000000000000ULL, 0x3170000000000000ULL, 0x33A0000000000000ULL, 0x3210000000000000ULL, 
        0x3B80000000000000ULL, 0x3A30000000000000ULL, 0x38E0000000000000ULL, 0x3950000000000000ULL, 
        0x3D40000000000000ULL, 0x3CF0000000000000ULL, 0x3E20000000000000ULL, 0x3F90000000000000ULL, 
        0x2D00000000000000ULL, 0x2CB0000000000000ULL, 0x2E60000000000000ULL, 0x2FD0000000000000ULL, 
        0x2BC0000000000000ULL, 0x2A70000000000000ULL, 0x28A0000000000000ULL, 0x2910000000000000ULL, 
        0x2080000000000000ULL, 0x2130000000000000ULL, 0x23E0000000000000ULL, 0x2250000000000000ULL, 
        0x2640000000000000ULL, 0x27F0000000000000ULL, 0x2520000000000000ULL, 0x2490000000000000ULL, 
        0x6C00000000000000ULL, 0x6DB0000000000000ULL, 0x6F60000000000000ULL, 0x6ED0000000000000ULL, 
        0x6AC0000000000000ULL, 0x6B70000000000000ULL, 0x69A0000000000000ULL, 0x6810000000000000ULL, 
        0x6180000000000000ULL, 0x6030000000000000ULL, 0x62E0000000000000ULL, 0x6350000000000000ULL, 
        0x6740000000000000ULL, 0x66F0000000000000ULL, 0x6420000000000000ULL, 0x6590000000000000ULL, 
        0x7700000000000000ULL, 0x76B0000000000000ULL, 0x7460000000000000ULL, 0x75D0000000000000ULL, 
        0x71C0000000000000ULL, 0x7070000000000000ULL, 0x72A0000000000000ULL, 0x7310000000000000ULL, 
        0x7A80000000000000ULL, 0x7B30000000000000ULL, 0x79E0000000000000ULL, 0x7850000000000000ULL, 
        0x7C40000000000000ULL, 0x7DF0000000000000ULL, 0x7F20000000000000ULL, 0x7E90000000000000ULL, 
        0x5A00000000000000ULL, 0x5BB0000000000000ULL, 0x5960000000000000ULL, 0x58D0000000000000ULL, 
        0x5CC0000000000000ULL, 0x5D70000000000000ULL, 0x5FA0000000000000ULL, 0x5E10000000000000ULL, 
        0x5780000000000000ULL, 0x5630000000000000ULL, 0x54E0000000000000ULL, 0x5550000000000000ULL, 
        0x5140000000000000ULL, 0x50F0000000000000ULL, 0x5220000000000000ULL, 0x5390000000000000ULL, 
        0x4100000000000000ULL, 0x40B0000000000000ULL, 0x4260000000000000ULL, 0x43D0000000000000ULL, 
        0x47C0000000000000ULL, 0x4670000000000000ULL, 0x44A0000000000000ULL, 0x4510000000000000ULL, 
        0x4C80000000000000ULL, 0x4D30000000000000ULL, 0x4FE0000000000000ULL, 0x4E50000000000000ULL, 
        0x4A40000000000000ULL, 0x4BF0000000000000ULL, 0x4920000000000000ULL, 0x4890000000000000ULL, 
        0xD800000000000000ULL, 0xD9B0000000000000ULL, 0xDB60000000000000ULL, 0xDAD0000000000000ULL, 
        0xDEC0000000000000ULL, 0xDF70000000000000ULL, 0xDDA0000000000000ULL, 0xDC10000000000000ULL, 
        0xD580000000000000ULL, 0xD430000000000000ULL, 0xD6E0000000000000ULL, 0xD750000000000000ULL, 
        0xD340000000000000ULL, 0xD2F0000000000000ULL, 0xD020000000000000ULL, 0xD190000000000000ULL, 
        0xC300000000000000ULL, 0xC2B0000000000000ULL, 0xC060000000000000ULL, 0xC1D0000000000000ULL, 
        0xC5C0000000000000ULL, 0xC470000000000000ULL, 0xC6A0000000000000ULL, 0xC710000000000000ULL, 
        0xCE80000000000000ULL, 0xCF30000000000000ULL, 0xCDE0000000000000ULL, 0xCC50000000000000ULL, 
        0xC840000000000000ULL, 0xC9F0000000000000ULL, 0xCB20000000000000ULL, 0xCA90000000000000ULL, 
        0xEE00000000000000ULL, 0xEFB0000000000000ULL, 0xED60000000000000ULL, 0xECD0000000000000ULL, 
        0xE8C0000000000000ULL, 0xE970000000000000ULL, 0xEBA0000000000000ULL, 0xEA10000000000000ULL, 
        0xE380000000000000ULL, 0xE230000000000000ULL, 0xE0E0000000000000ULL, 0xE150000000000000ULL, 
        0xE540000000000000ULL, 0xE4F0000000000000ULL, 0xE620000000000000ULL, 0xE790000000000000ULL, 
        0xF500000000000000ULL, 0xF4B0000000000000ULL, 0xF660000000000000ULL, 0xF7D0000000000000ULL, 
        0xF3C0000000000000ULL, 0xF270000000000000ULL, 0xF0A0000000000000ULL, 0xF110000000000000ULL, 
        0xF880000000000000ULL, 0xF930000000000000ULL, 0xFBE0000000000000ULL, 0xFA50000000000000ULL, 
        0xFE40000000000000ULL, 0xFFF0000000000000ULL, 0xFD20000000000000ULL, 0xFC90000000000000ULL, 
        0xB400000000000000ULL, 0xB5B0000000000000ULL, 0xB760000000000000ULL, 0xB6D0000000000000ULL, 
        0xB2C0000000000000ULL, 0xB370000000000000ULL, 0xB1A0000000000000ULL, 0xB010000000000000ULL, 
        0xB980000000000000ULL, 0xB830000000000000ULL, 0xBAE0000000000000ULL, 0xBB50000000000000ULL, 
        0xBF40000000000000ULL, 0xBEF0000000000000ULL, 0xBC20000000000000ULL, 0xBD90000000000000ULL, 
        0xAF00000000000000ULL, 0xAEB0000000000000ULL, 0xAC60000000000000ULL, 0xADD0000000000000ULL, 
        0xA9C0000000000000ULL, 0xA870000000000000ULL, 0xAAA0000000000000ULL, 0xAB10000000000000ULL, 
        0xA280000000000000ULL, 0xA330000000000000ULL, 0xA1E0000000000000ULL, 0xA050000000000000ULL, 
        0xA440000000000000ULL, 0xA5F0000000000000ULL, 0xA720000000000000ULL, 0xA690000000000000ULL, 
        0x8200000000000000ULL, 0x83B0000000000000ULL, 0x8160000000000000ULL, 0x80D0000000000000ULL, 
        0x84C0000000000000ULL, 0x8570000000000000ULL, 0x87A0000000000000ULL, 0x8610000000000000ULL, 
        0x8F80000000000000ULL, 0x8E30000000000000ULL, 0x8CE0000000000000ULL, 0x8D50000000000000ULL, 
        0x8940000000000000ULL, 0x88F0000000000000ULL, 0x8A20000000000000ULL, 0x8B90000000000000ULL, 
        0x9900000000000000ULL, 0x98B0000000000000ULL, 0x9A60000000000000ULL, 0x9BD0000000000000ULL, 
        0x9FC0000000000000ULL, 0x9E70000000000000ULL, 0x9CA0000000000000ULL, 0x9D10000000000000ULL, 
        0x9480000000000000ULL, 0x9530000000000000ULL, 0x97E0000000000000ULL, 0x9650000000000000ULL, 
        0x9240000000000000ULL, 0x93F0000000000000ULL, 0x9120000000000000ULL, 0x9090000000000000ULL 
    }; 
    //since all low 48 bit of the table element are 0, we can use only the high 16 bit element for optimization
     static const uint16_t s_optimized_crc64_table[] = 
     {
         0x0000, 0x01b0, 0x0360, 0x02d0,
         0x06c0, 0x0770, 0x05a0, 0x0410,
         0x0d80, 0x0c30, 0x0ee0, 0x0f50,
         0x0b40, 0x0af0, 0x0820, 0x0990,
         0x1b00, 0x1ab0, 0x1860, 0x19d0,
         0x1dc0, 0x1c70, 0x1ea0, 0x1f10,
         0x1680, 0x1730, 0x15e0, 0x1450,
         0x1040, 0x11f0, 0x1320, 0x1290,
         0x3600, 0x37b0, 0x3560, 0x34d0,
         0x30c0, 0x3170, 0x33a0, 0x3210,
         0x3b80, 0x3a30, 0x38e0, 0x3950,
         0x3d40, 0x3cf0, 0x3e20, 0x3f90,
         0x2d00, 0x2cb0, 0x2e60, 0x2fd0,
         0x2bc0, 0x2a70, 0x28a0, 0x2910,
         0x2080, 0x2130, 0x23e0, 0x2250,
         0x2640, 0x27f0, 0x2520, 0x2490,
         0x6c00, 0x6db0, 0x6f60, 0x6ed0,
         0x6ac0, 0x6b70, 0x69a0, 0x6810,
         0x6180, 0x6030, 0x62e0, 0x6350,
         0x6740, 0x66f0, 0x6420, 0x6590,
         0x7700, 0x76b0, 0x7460, 0x75d0,
         0x71c0, 0x7070, 0x72a0, 0x7310,
         0x7a80, 0x7b30, 0x79e0, 0x7850,
         0x7c40, 0x7df0, 0x7f20, 0x7e90,
         0x5a00, 0x5bb0, 0x5960, 0x58d0,
         0x5cc0, 0x5d70, 0x5fa0, 0x5e10,
         0x5780, 0x5630, 0x54e0, 0x5550,
         0x5140, 0x50f0, 0x5220, 0x5390,
         0x4100, 0x40b0, 0x4260, 0x43d0,
         0x47c0, 0x4670, 0x44a0, 0x4510,
         0x4c80, 0x4d30, 0x4fe0, 0x4e50,
         0x4a40, 0x4bf0, 0x4920, 0x4890,
         0xd800, 0xd9b0, 0xdb60, 0xdad0,
         0xdec0, 0xdf70, 0xdda0, 0xdc10,
         0xd580, 0xd430, 0xd6e0, 0xd750,
         0xd340, 0xd2f0, 0xd020, 0xd190,
         0xc300, 0xc2b0, 0xc060, 0xc1d0,
         0xc5c0, 0xc470, 0xc6a0, 0xc710,
         0xce80, 0xcf30, 0xcde0, 0xcc50,
         0xc840, 0xc9f0, 0xcb20, 0xca90,
         0xee00, 0xefb0, 0xed60, 0xecd0,
         0xe8c0, 0xe970, 0xeba0, 0xea10,
         0xe380, 0xe230, 0xe0e0, 0xe150,
         0xe540, 0xe4f0, 0xe620, 0xe790,
         0xf500, 0xf4b0, 0xf660, 0xf7d0,
         0xf3c0, 0xf270, 0xf0a0, 0xf110,
         0xf880, 0xf930, 0xfbe0, 0xfa50,
         0xfe40, 0xfff0, 0xfd20, 0xfc90,
         0xb400, 0xb5b0, 0xb760, 0xb6d0,
         0xb2c0, 0xb370, 0xb1a0, 0xb010,
         0xb980, 0xb830, 0xbae0, 0xbb50,
         0xbf40, 0xbef0, 0xbc20, 0xbd90,
         0xaf00, 0xaeb0, 0xac60, 0xadd0,
         0xa9c0, 0xa870, 0xaaa0, 0xab10,
         0xa280, 0xa330, 0xa1e0, 0xa050,
         0xa440, 0xa5f0, 0xa720, 0xa690,
         0x8200, 0x83b0, 0x8160, 0x80d0,
         0x84c0, 0x8570, 0x87a0, 0x8610,
         0x8f80, 0x8e30, 0x8ce0, 0x8d50,
         0x8940, 0x88f0, 0x8a20, 0x8b90,
         0x9900, 0x98b0, 0x9a60, 0x9bd0,
         0x9fc0, 0x9e70, 0x9ca0, 0x9d10,
         0x9480, 0x9530, 0x97e0, 0x9650,
         0x9240, 0x93f0, 0x9120, 0x9090 
    };
    */
    
    #define DO_1_STEP(uCRC64, pu8) uCRC64 = s_crc64_table[(uCRC64 ^ *pu8++) & 0xff] ^ (uCRC64 >> 8); 
    #define DO_2_STEP(uCRC64, pu8)  DO_1_STEP(uCRC64, pu8); DO_1_STEP(uCRC64, pu8); 
    #define DO_4_STEP(uCRC64, pu8)  DO_2_STEP(uCRC64, pu8); DO_2_STEP(uCRC64, pu8); 
    #define DO_8_STEP(uCRC64, pu8)  DO_4_STEP(uCRC64, pu8); DO_4_STEP(uCRC64, pu8); 
    #define DO_16_STEP(uCRC64, pu8)  DO_8_STEP(uCRC64, pu8); DO_8_STEP(uCRC64, pu8); 
    
    #ifdef __x86_64__
    #define DO_1_OPTIMIZED_STEP(uCRC64, pu8)  \
                __asm__ __volatile__(   \
        "movq (%1),%%mm0\n\t"  \
        "pxor  %0, %%mm0\n\t"   \
        "pextrw $0, %%mm0, %%eax\n\t"   \
            "movzbq %%al, %%r8\n\t" \
        "movzwl (%2,%%r8,2), %%ecx\n\t"        \
            "pinsrw $0, %%ecx, %%mm3\n\t"      \
        "pextrw $3, %%mm0, %%ebx\n\t"   \
            "xorb %%bh, %%cl\n\t" \
            "movzbq %%cl, %%r8\n\t" \
        "movzwl (%2,%%r8,2), %%edx\n\t" \
        "pinsrw $3, %%edx, %%mm4\n\t"    \
            "movb %%ah, %%al\n\t"   \
            "movzbq %%al, %%r8\n\t" \
            "movzwl (%2, %%r8,2), %%ecx\n\t"    \
        "pinsrw $0, %%ecx, %%mm4\n\t"    \
            "movzbq %%bl, %%r8\n\t" \
        "movzwl (%2, %%r8,2), %%edx\n\t"       \
        "pinsrw $3, %%edx, %%mm3\n\t"    \
        "pextrw $1, %%mm0, %%eax\n\t"   \
            "movzbq %%al, %%r8\n\t" \
        "movzwl (%2, %%r8,2), %%ecx\n\t" \
        "pinsrw $1, %%ecx, %%mm3\n\t"    \
            "movb %%ah, %%al\n\t"   \
            "movzbq %%al, %%r8\n\t" \
        "movzwl (%2, %%r8,2), %%edx\n\t" \
        "pinsrw $1, %%edx, %%mm4\n\t"    \
        "pextrw $2, %%mm0, %%ebx\n\t"   \
            "movzbq %%bl, %%r8\n\t" \
        "movzwl (%2, %%r8,2), %%ecx\n\t" \
        "pinsrw $2, %%ecx, %%mm3\n\t"    \
            "movb %%bh, %%al\n\t"   \
            "movzbq %%al, %%r8\n\t" \
        "movzwl (%2, %%r8,2), %%edx\n\t" \
        "pinsrw $2, %%edx, %%mm4\n\t"    \
            "psrlq $8, %%mm3\n\t" \
        "pxor %%mm3, %%mm4\n\t" \
        "movq %%mm4, %0\n\t"    \
        :"+&y" (uCRC64) \
        :"r"(pu8),"D"(s_optimized_crc64_table)   \
        :"eax","ebx","ecx","edx","mm0","mm3","mm4","r8");  
    #else
    #define DO_1_OPTIMIZED_STEP(uCRC64, pu8, u_data, operand1, operand2, table_result0)  \
                u_data = uCRC64 ^ (*(uint64_t*)pu8); \
                table_result0 = s_optimized_crc64_table[(uint8_t)u_data];    \
                operand1 = ((uint64_t)s_optimized_crc64_table[(uint8_t)(u_data >> 48)] << 40)     \
                                    |((uint64_t)s_optimized_crc64_table[(uint8_t)(u_data >> 32)] << 24)   \
                                    | ((uint64_t)s_optimized_crc64_table[(uint8_t)(u_data >> 16)] << 8)   \
                                    |( (uint64_t)table_result0 >> 8 );   \
                operand2 = ((uint64_t)s_optimized_crc64_table[(uint8_t)(u_data >> 56) ^ ((uint8_t)table_result0)] << 48)   \
                                    |((uint64_t)s_optimized_crc64_table[(uint8_t)(u_data >> 40)] << 32)   \
                                    | ((uint64_t)s_optimized_crc64_table[(uint8_t)(u_data >> 24)] << 16)  \
                                    |(s_optimized_crc64_table[(uint8_t)(u_data >> 8)]);  \
                uCRC64 = operand1 ^ operand2;   
    #endif            
    /** 
      * Processes a multiblock of a CRC64 calculation. 
      * 
      * @returns Intermediate CRC64 value. 
      * @param   uCRC64  Current CRC64 intermediate value. 
      * @param   pv      The data block to process. 
      * @param   cb      The size of the data block in bytes. 
      */ 
    uint64_t ob_crc64_optimized(uint64_t uCRC64, const void *pv, int64_t cb) 
    { 
        const uint8_t *pu8 = (const uint8_t *)pv; 
    #ifndef __x86_64__
        uint64_t u_data = 0, operand1 = 0, operand2 = 0;
        uint16_t table_result0 = 0;
    #endif    
        
        if ( pv != NULL && cb != 0 )
        {
            while ( cb >= 64 )
            {
    #ifdef __x86_64__
                DO_1_OPTIMIZED_STEP(uCRC64, pu8);
                pu8 += 8;
                DO_1_OPTIMIZED_STEP(uCRC64, pu8);
                pu8 += 8;
                DO_1_OPTIMIZED_STEP(uCRC64, pu8);
                pu8 += 8;
                DO_1_OPTIMIZED_STEP(uCRC64, pu8);
                pu8 += 8;
                DO_1_OPTIMIZED_STEP(uCRC64, pu8);
                pu8 += 8;
                DO_1_OPTIMIZED_STEP(uCRC64, pu8);
                pu8 += 8;
                DO_1_OPTIMIZED_STEP(uCRC64, pu8);
                pu8 += 8;
                DO_1_OPTIMIZED_STEP(uCRC64, pu8);
                pu8 += 8;
    
    #else                        
                DO_1_OPTIMIZED_STEP(uCRC64, pu8, u_data, operand1, operand2, table_result0);
                pu8 += 8;
                DO_1_OPTIMIZED_STEP(uCRC64, pu8, u_data, operand1, operand2, table_result0);
                pu8 += 8;
                DO_1_OPTIMIZED_STEP(uCRC64, pu8, u_data, operand1, operand2, table_result0);
                pu8 += 8;
                DO_1_OPTIMIZED_STEP(uCRC64, pu8, u_data, operand1, operand2, table_result0);
                pu8 += 8;
                DO_1_OPTIMIZED_STEP(uCRC64, pu8, u_data, operand1, operand2, table_result0);
                pu8 += 8;
                DO_1_OPTIMIZED_STEP(uCRC64, pu8, u_data, operand1, operand2, table_result0);
                pu8 += 8;
                DO_1_OPTIMIZED_STEP(uCRC64, pu8, u_data, operand1, operand2, table_result0);
                pu8 += 8;
                DO_1_OPTIMIZED_STEP(uCRC64, pu8, u_data, operand1, operand2, table_result0);
                pu8 += 8;
    #endif            
                cb -= 64;
            }
    #ifdef __x86_64__
            __asm__ __volatile__(
                "emms"
            );
    #endif
            while ( cb-- ) 
            {
                DO_1_STEP(uCRC64, pu8);
            }
        }
    
        return uCRC64; 
    } 
    /** 
      * Processes a multiblock of a CRC64 calculation. 
      * 
      * @returns Intermediate CRC64 value. 
      * @param   uCRC64  Current CRC64 intermediate value. 
      * @param   pv      The data block to process. 
      * @param   cb      The size of the data block in bytes. 
      */ 
    uint64_t ob_crc64(uint64_t uCRC64, const void *pv, int64_t cb) 
    { 
        /*const uint8_t *pu8 = (const uint8_t *)pv; 
         
        if ( pv != NULL && cb != 0 )
        {
            while ( cb >= 8 )
            {
                DO_8_STEP( uCRC64, pu8);
                cb -= 8;
            }
    
            while ( cb-- ) 
            {
                DO_1_STEP(uCRC64, pu8);
            }
        }
    
        return uCRC64; */
        return ob_crc64_optimized(uCRC64, pv, cb);
    } 
    
    /** 
      * Calculate CRC64 for a memory block. 
      * 
      * @returns CRC64 for the memory block. 
      * @param   pv      Pointer to the memory block. 
      * @param   cb      Size of the memory block in bytes. 
      */ 
    uint64_t ob_crc64(const void *pv, int64_t cb) 
    { 
         uint64_t  uCRC64 = 0ULL;
         return ob_crc64( uCRC64, pv, cb);
    } 
    
    /**
      * Get the static CRC64 table. This function is only used for testing purpose.
      *
      */
    const uint64_t * ob_get_crc64_table()
    {
        return s_crc64_table;
    }
  
  }
}

