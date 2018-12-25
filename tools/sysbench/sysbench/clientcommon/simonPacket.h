///////////////////////////////////////////////////////////////////////////////
/// \file
/// Code related to a Simon client's connection to an aggregation server.
///
/// Author: dwind
// Copyright 2005 Yahoo Inc. All Rights Reserved. Confidential.

#ifndef SIMONPACKET_H_
#define SIMONPACKET_H_

#include "simonUtils.h"

namespace simon {

///////////////////////////////////////////////////////////////////////////////
/// Represents a Simon UDP packet
/// Mostly a buffer that knows how to reset w/o affecting header info.
/// Also owns formatting headers according the Simon protocol and 
//  provides related utility funtions.
///////////////////////////////////////////////////////////////////////////////
class SimonPacketC {

    typedef std::list<ustring> UStringListC;

 public:
	// constants from protocol definition:
	static const UStringListC::size_type kMaxBlurbMsgSigs= 1;
	static const UStringListC::size_type kMaxReportSigs= 15;
	static const unsigned char kMagicNumber1= 'S';
	static const unsigned char kMagicNumber2= 'I';
	static const unsigned char kSimonProtocolVersion= 2;

        // HACK: GCC 4.1.1 gives undefined reference errors if the following 
        // two lines are used instead of the #defines.  No idea why. They work
        // fine under GCC 3.4.3.
	//static const size_t kMaxBlurbPacketSize= 1400;
        //static const size_t kMaxConfigPacketSize= 65536;
        #define kMaxBlurbPacketSize 1400
        #define kMaxConfigPacketSize 65536
	
	/// Writes the Simon form of the string in src to dest.
	/// \param dest a reference to a pointer, which is modified & advanced.
	/// \param src the input chars to be serialized
	/// \return void because output is stored in dest
	static void serializeString(unsigned char *&dest, const char *src);
	static void serializeString(unsigned char *&dest, std::string src);
	
	static void serializeStringToUString(ustring &dest, const char *src);
	
	/// Writes the Simon form of the int value to dest.
	/// \param dest a reference to a pointer, which is modified & advanced.
	/// \param value the unsigned intb to be serialized
	/// \return void because output is stored in dest
	static void serializeUint32(unsigned char *&dest, uint32_t value);
	static void serializeUint64(unsigned char *&dest, uint64_t value);

	SimonPacketC(std::string appName, std::string clusterName, 
                     std::string nodeName, const UStringListC &blurbMsgSigs, 
                     const UStringListC &reportSigs, uint64_t timestamp);
		
	~SimonPacketC() { delete[] mBuf; }

	int getLength() { return mCurPtr- mBuf; }
	bool isEmpty() { return mCurPtr== mMinPtr; }
	
	// TODO DWind: Returning private ptr breaks encapsulation
	unsigned char *getData() { return mBuf; } 
	
	bool append(const unsigned char *src, int len);
	
	void reset();
	void incrPacketCounter();
	
 private:
	unsigned char *mBuf;
	unsigned char *mCurPtr;
	unsigned char *mMinPtr;
	unsigned char *mMaxPtr;	
        unsigned char *mPacketCounterPtr;
};

} // namespace

#endif /*SIMONPACKET_H_*/
