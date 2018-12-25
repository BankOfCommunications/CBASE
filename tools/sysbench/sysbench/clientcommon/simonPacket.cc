#include "simonPacket.h"
#include "simonClient.h"

using namespace std;
using namespace simon;

///////////////////////////////////////////////////////////////////////////////
//-------------------------class SimonPacketC----------------------------------
///////////////////////////////////////////////////////////////////////////////


//-------------------------------------------------------------------------
// SimonPacketC::serializeString()
//
// Serializes string to an unsigned char buffer, writing its length first,
// followed by the string characters without any null terminator. Strings over 
// 255 in length are truncated.
//-------------------------------------------------------------------------
void SimonPacketC::serializeString(unsigned char *&dest, const char *src) {
    size_t len = strlen(src);
    if (len > 255) {
        len = 255;
    }
    *dest++ = static_cast<unsigned char>(len);
    for(size_t i=0; i<len; i++) {
        *dest++ = static_cast<unsigned char>(*src++);
    }
}

void SimonPacketC::serializeString(unsigned char *&dest, const string src) {
        serializeString(dest, src.c_str());
}

//-------------------------------------------------------------------------
// SimonPacketC::serializeStringToUString()
//
// Serializes string to a ustring object. Strings over 255 in length are 
// truncated.
//-------------------------------------------------------------------------
void SimonPacketC::serializeStringToUString(ustring &dest, const char *src) {
    size_t len = strlen(src);
    if (len > 255) {
        len = 255;
    }
    dest += static_cast<unsigned char>(len);
    for(size_t i=0; i<len; i++) {
        dest += static_cast<unsigned char>(*src++);
    }
}


//-------------------------------------------------------------------------
// SimonPacketC::serializeUint32()
// WARNING: assumes little-endian machine architecture
//-------------------------------------------------------------------------
void SimonPacketC::serializeUint32(unsigned char *&dest, uint32_t value) {
        uint32_t *dest32 = (uint32_t*) dest;
        *dest32++ = value;
        dest = (unsigned char *) dest32;
}


//-------------------------------------------------------------------------
// SimonPacketC::serializeUint64()
// WARNING: assumes little-endian machine architecture
//-------------------------------------------------------------------------
void SimonPacketC::serializeUint64(unsigned char *&dest, uint64_t value) {
        uint64_t *dest64 = (uint64_t*) dest;
        *dest64++ = value;
        dest = (unsigned char *) dest64;
}


//-------------------------------------------------------------------------
// SimonPacketC::SimonPacketC(int) constructor
//-------------------------------------------------------------------------
SimonPacketC::SimonPacketC(string appName, 
                           string clusterName, string nodeName, 
                           const UStringListC &blurbMsgSigs, const UStringListC &reportSigs,
                           uint64_t timestamp) {

        bool isConfig = (reportSigs.size() > 0); // used for config messages
        int maxPacketSize = (isConfig ? kMaxConfigPacketSize : kMaxBlurbPacketSize);

	mBuf= new unsigned char[maxPacketSize];
	mMinPtr= mCurPtr= mBuf;
	mMaxPtr= mBuf+ maxPacketSize;
		
	*mCurPtr++= kMagicNumber1;
	*mCurPtr++= kMagicNumber2;
	*mCurPtr++= kSimonProtocolVersion;
	
	// set counts byte:
	*mCurPtr= 0x00;
	if(isConfig) 
		*mCurPtr |= 0x80;
	*mCurPtr|= ((blurbMsgSigs.size() << 4) & 0x70);
	*mCurPtr|= (reportSigs.size()  & 0x0f);
	mCurPtr++;
	
	serializeString(mCurPtr, appName );
	serializeString(mCurPtr, clusterName );
	serializeString(mCurPtr, nodeName );
        serializeUint32(mCurPtr, getpid());
	
	UStringListC::const_iterator iter= blurbMsgSigs.begin();
	for(; iter!=blurbMsgSigs.end(); iter++) {
		ustring::const_iterator uIter= iter->begin();
		for(; uIter!= iter->end(); uIter++) 
		  *mCurPtr++= *uIter;
	}
	
	iter= reportSigs.begin();
	for(; iter!=reportSigs.end(); iter++) {
		ustring::const_iterator uIter= iter->begin();
		for(; uIter!= iter->end(); uIter++) 
		  *mCurPtr++= *uIter;
	}
	
	mPacketCounterPtr = 0;
        if (isConfig) {
                serializeUint64(mCurPtr, timestamp);
        }
	else {
	        mPacketCounterPtr = mCurPtr++; // allocate a byte for the packet counter
	        *mPacketCounterPtr = 0;        // initialize packet counter
	}
	mMinPtr= mCurPtr;
}
	
//-------------------------------------------------------------------------
// SimonPacketC::append()
//-------------------------------------------------------------------------
bool SimonPacketC::append(const unsigned char *src, int len) {
	if(mCurPtr+ len > mMaxPtr) 
		return false;
	
	unsigned char *end;
	for(end= mCurPtr+len; mCurPtr<end; mCurPtr++) 
		*mCurPtr= *src++;
	return true;
}

//-------------------------------------------------------------------------
// Resets the packet to its empty state (the header part being left intact)
//-------------------------------------------------------------------------
void SimonPacketC::reset() { 
        mCurPtr= mMinPtr; 
}

//-------------------------------------------------------------------------
// Increments the packet counter (if this is a regular rather than a config packet)
//-------------------------------------------------------------------------
void SimonPacketC::incrPacketCounter() {
	if (mPacketCounterPtr) {
	        (*mPacketCounterPtr)++;
	}
}
