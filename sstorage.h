#pragma once

#ifndef __IDEMPOTENT_TRANSACTION_COUNTING_SSTORAGE_H_
#define __IDEMPOTENT_TRANSACTION_COUNTING_SSTORAGE_H_

#include "boost/noncopyable.hpp"

namespace structuredstorage_ns
{
    class StructuredStorage;

    enum
    {
        SS_SUCCESS = 0,
        SS_ERROR = 1,           // General error
        SS_EOF = 2,             // End of file (End of stream)
        SS_NOPAGES = 3,         // Internal, no more pages in the stream
        SS_INVALID_STREAM = 4,  // Invalid streamid
        SS_SEEK_RANGE = 5,      // Attempt to seek out of range
        SS_EXISTS = 6,          // Stream name exists
        SS_NOT_A_STORAGE,       // Open failed, not a storage file
        SS_UNKNOWN_VERSION,     // Unsupported version
        SS_NOT_OPENED,          // Storage is not opened
        SS_ALREADY_OPENED,      // Storage is already opened
        SS_NOT_FOUND            // Stream name not found
    };

    class Position
    {
    private:
        int fileOffsetPage;
        int offsetInPage;
        int streamOffset;
        friend StructuredStorage;
    };

    class StructuredStorage
    {
    public:
        StructuredStorage();
        ~StructuredStorage();
        // Open a storage file
        int OpenStorage(const char *filename);

        // Create a storage file
        int CreateStorage(const char *filename, int pageSize = 1024);

        // Close the storage file
        int CloseStorage();

        // Create a stream
        int CreateStream(const char *name, int& streamid);

        // Open a stream
        int OpenStream(const char *name, int& streamid);

        // read data from a stream
        int Read(int streamid, char *buf, int bytesToRead, int& bytesRead);

        // Write data to a stream
        int Write(int streamid, const char *buf, int bytesToWrite);

        // Seek in a stream
        // This can be a costly operation since it must walk the chain
        // of pages to locate the offset
        int StreamSeek(int streamid, int streamOffset);

        // Get the stream position
        int StreamPosition(int streamid, int& pos);

        //FilePosition are much faster then stream positions. However
        // you cannot manipulate the position
        int FileSeek(int streamid, const Position& pos);

        // Get the current file position of the given stream
        int FilePosition(int streamid, Position& pos);
    private:
        struct fileheader
        {
            int magic;
            int version;
            int fileOffsetFirstFreePage;    // First page on free list
            int fileOffsetFirstPageStream0; // First page of stream 0. The first page probably
                                            // immediately follows this header
            int numstreams;                 // Number of streams in this storage
            int pageSize;                   // Page size for this storage file
        };

        struct pageheader
        {
            int streamid;          // -1 page is free
            int usedBytes;       // Number of bytes used in tis page
            int fileOffsetNextPage;  // Offset of the next page in this stream
            int fileOffsetThisPage;  // File offset of this page
        };
        
        enum
        {
            MAGIC_NUM = 0xff783445,
            VERSION_NUM = 1,
            STREAM0 = 0,
            MAX_STREAM_NAME = 32,
        };

        struct streamInfo
        {
            int streamid;
            char name[MAX_STREAM_NAME];
            int fileOffsetPage0;    // For this stream, the file offset of the first page
            int streamsize;     // Number of bytes in this stream
        };
        struct Stream
        {
            streamInfo info;
            pageheader currentPage;
            char *pageData;
            int currentStreamPos;
            int currentPagePos;     // 0 thru pageheader.usedbytes-1
            bool dirty;             // Needs to be written
        };

        int m_fd;
        typedef  std::map<int, Stream> streammap_t;
        streammap_t m_streams;     // stream id, stream
        fileheader m_header;
        int m_pageDataSize;
    private:
        int loadStreams();
        int writeStorageHeader();
        int readStorageHeader();
        int readPageHeader(int offset, pageheader& pheader);
        int writePageHeader(pageheader& pheader);
        int readPageData(pageheader&, char *buf);
        int writePageData(pageheader&, const char *buf);
        int loadNextPage(Stream& strm);
        int readblock(Stream& strm, char *buf, int bytesToRead);
        int writeblock(Stream& strm, const char *buf, int bytesToWrite);
        int writeCurrentPage(Stream& strm);
        int allocNewPage(Stream& strm);
        int allocNewPageFromFreeList(Stream& strm);
        int allocNewPageFromDisk(Stream& strm);
        int flushStreamDirectory();
    };
}

#endif // __IDEMPOTENT_TRANSACTION_COUNTING_SSTORAGE_H_

