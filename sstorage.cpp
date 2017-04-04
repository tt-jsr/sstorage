#include "pch.h"
#include "sstorage.h"
#include <io.h>
#include <fcntl.h>
#include <sys/stat.h>

using namespace std;
using namespace tt_core_ns;

namespace structuredstorage_ns
{
    StructuredStorage::StructuredStorage()
        :m_fd(-1)
    {

    }

    StructuredStorage::~StructuredStorage()
    {
        CloseStorage();
    }


    int StructuredStorage::Read(int stream, char *buf, int bytesToRead, int& bytesRead)
    {
        if (m_fd == -1)
        {
            return SS_NOT_OPENED;
        }
        char *dst = buf;
        streammap_t::iterator it = m_streams.find(stream);
        if (it == m_streams.end())
            return SS_INVALID_STREAM;
        Stream& strm = (*it).second;
        bytesRead = 0;
        while (bytesToRead)
        {
            // The number of unread bytes in this page that could be read
            int unreadBytesInPage = strm.currentPage.usedBytes - strm.currentPagePos;
            if (unreadBytesInPage == 0)
            {
                if (loadNextPage(strm) != 0)
                    return SS_EOF;
            }
            else
            {
                // The number of bytes we want to read out of this page
                int bytesInPageToRead = bytesToRead < unreadBytesInPage ? bytesToRead : unreadBytesInPage;
                readblock(strm, dst, bytesInPageToRead);
                bytesToRead -= bytesInPageToRead;
                bytesRead += bytesInPageToRead;
                dst += bytesInPageToRead;
            }
        }
        return SS_SUCCESS;
    }

    int StructuredStorage::CloseStorage()
    {
        if (m_fd == -1)
        {
            return SS_NOT_OPENED;
        }
        flushStreamDirectory();
        // Write all the stream info's and current pages and data
        streammap_t::iterator it = m_streams.begin();
        streammap_t::iterator eit = m_streams.end();
        while (it != eit)
        {
            Stream& strm = (*it).second;
            if (strm.dirty)
            {
                writePageHeader(strm.currentPage);
                writePageData(strm.currentPage, strm.pageData);
            }
            delete strm.pageData;
            strm.pageData = nullptr;
            ++it;
        }

        m_streams.clear();
        writeStorageHeader();

        _close(m_fd);
        m_fd = -1;
        return SS_SUCCESS;
    }

    int StructuredStorage::OpenStorage(const char *filename)
    {
        if (m_fd != -1)
        {
            return SS_ALREADY_OPENED;
        }
        m_fd = _open(filename, O_RDWR);
        if (m_fd < 0)
        {
            return SS_ERROR;
        }
        readStorageHeader();
        if (m_header.magic != MAGIC_NUM)
        {
            _close(m_fd);
            return SS_NOT_A_STORAGE;
        }
        if (m_header.version != VERSION_NUM)
        {
            _close(m_fd);
            return SS_UNKNOWN_VERSION;
        }
        m_pageDataSize = m_header.pageSize - sizeof(pageheader);
        loadStreams();
        return SS_SUCCESS;
    }

    int StructuredStorage::CreateStorage(const char *filename, int pageSize)
    {
        if (m_fd != -1)
        {
            return SS_ALREADY_OPENED;
        }
        m_fd = _open(filename, O_RDWR | O_CREAT | O_TRUNC, _S_IREAD | _S_IWRITE);
        if (m_fd < 0)
        {
            return SS_ERROR;
        }
        m_header.magic = MAGIC_NUM;
        m_header.version = VERSION_NUM;
        m_header.fileOffsetFirstFreePage = 0;
        m_header.fileOffsetFirstPageStream0 = sizeof(fileheader);
        m_header.numstreams = 0;
        m_header.pageSize = pageSize;
        writeStorageHeader();

        m_pageDataSize = m_header.pageSize - sizeof(pageheader);
        int streamid;
        int r = CreateStream("PaGiNgSyStEm", streamid);
        TT_ASSERT(streamid == STREAM0);
        TT_ASSERT(r == SS_SUCCESS);
        return r;
    }


    int StructuredStorage::Write(int stream, const char *buf, int bytesToWrite)
    {
        if (m_fd == -1)
        {
            return SS_NOT_OPENED;
        }
        const char *src = buf;
        streammap_t::iterator it = m_streams.find(stream);
        if (it == m_streams.end())
            return SS_INVALID_STREAM;
        Stream& strm = (*it).second;
        while (bytesToWrite)
        {
            // The number of bytes in this page that could be written
            int unwrittenBytesInPage = m_pageDataSize - strm.currentPagePos;
            if (unwrittenBytesInPage == 0)
            {
                writeCurrentPage(strm);
                if (loadNextPage(strm) == SS_NOPAGES)
                {
                    allocNewPage(strm);
                }
            }
            else
            {
                // The number of bytes we want to write to this page
                int bytesToWriteToPage = bytesToWrite < unwrittenBytesInPage ? bytesToWrite : unwrittenBytesInPage;
                writeblock(strm, src, bytesToWriteToPage);
                bytesToWrite -= bytesToWriteToPage;
                src += bytesToWriteToPage;
            }
        }
        return SS_SUCCESS;
    }

    int StructuredStorage::FileSeek(int stream, const Position& pos)
    {
        if (m_fd == -1)
        {
            return SS_NOT_OPENED;
        }
        streammap_t::iterator it = m_streams.find(stream);
        if (it == m_streams.end())
            return SS_INVALID_STREAM;
        Stream& strm = (*it).second;
        if (strm.dirty)
        {
            writeCurrentPage(strm);
        }
        int r = readPageHeader(pos.fileOffsetPage, strm.currentPage);
        if (r != SS_SUCCESS)
        {
            return r;
        }
        r = readPageData(strm.currentPage, strm.pageData);
        if (r != SS_SUCCESS)
        {
            return r;
        }
        strm.currentPagePos = pos.offsetInPage;
        strm.currentStreamPos = pos.streamOffset;
        return SS_SUCCESS;
    }

    int StructuredStorage::FilePosition(int stream, Position& pos)
    {
        if (m_fd == -1)
        {
            return SS_NOT_OPENED;
        }
        streammap_t::iterator it = m_streams.find(stream);
        if (it == m_streams.end())
            return SS_INVALID_STREAM;
        Stream& strm = (*it).second;
        pos.fileOffsetPage = strm.currentPage.fileOffsetThisPage;
        pos.offsetInPage = strm.currentPagePos;
        pos.streamOffset = strm.currentStreamPos;
        return SS_SUCCESS;
    }

    int StructuredStorage::StreamSeek(int stream, int offset)
    {
        if (m_fd == -1)
        {
            return SS_NOT_OPENED;
        }
        streammap_t::iterator it = m_streams.find(stream);
        if (it == m_streams.end())
            return SS_INVALID_STREAM;
        Stream& strm = (*it).second;
        if (offset > strm.info.streamsize)
        {
            return SS_SEEK_RANGE;
        }

        // Walk the chain of page headers until we get the offset we want.
        pageheader pgheader;
        readPageHeader(strm.info.fileOffsetPage0, pgheader);
        int streamOffsetOfPage = 0;
        while (true)
        {
            if (offset >= streamOffsetOfPage && offset <= (streamOffsetOfPage+pgheader.usedBytes))
            {
                if (strm.dirty)
                {
                    writeCurrentPage(strm);
                }
                strm.currentPage = pgheader;
                readPageData(strm.currentPage, strm.pageData);
                strm.currentStreamPos = offset;
                strm.currentPagePos = offset - streamOffsetOfPage;
                TT_ASSERT(strm.currentPagePos <= strm.currentPage.usedBytes);
                return SS_SUCCESS;
            }
            streamOffsetOfPage += pgheader.usedBytes;
            TT_ASSERT(pgheader.fileOffsetNextPage != 0);
            readPageHeader(pgheader.fileOffsetNextPage, pgheader);
        }

        return SS_SUCCESS;
    }

    int StructuredStorage::StreamPosition(int stream, int& pos)
    {
        if (m_fd == -1)
        {
            return SS_NOT_OPENED;
        }
        streammap_t::iterator it = m_streams.find(stream);
        if (it == m_streams.end())
            return SS_INVALID_STREAM;
        Stream& strm = (*it).second;
        pos = strm.currentStreamPos;
        return SS_SUCCESS;
    }

    int StructuredStorage::CreateStream(const char *name, int& streamid)
    {
        if (m_fd == -1)
        {
            return SS_NOT_OPENED;
        }
        streammap_t::iterator it = m_streams.begin();
        streammap_t::iterator eit = m_streams.end();
        // Make sure namne is not is use
        while (it != eit)
        {
            if (strcmp((*it).second.info.name, name) == 0)
            {
                return SS_EXISTS;
            }
            ++it;
        }
        
        long pos = _lseek(m_fd, 0, SEEK_END);
        pageheader pgheader;
        pgheader.streamid = m_streams.size();
        pgheader.usedBytes = 0;
        pgheader.fileOffsetThisPage = pos;
        pgheader.fileOffsetNextPage = 0;

        writePageHeader(pgheader);
        _lseek(m_fd, m_pageDataSize-1, SEEK_CUR);
        int c = 0;
        _write(m_fd, &c, 1);    // Extend the file
        
        Stream strm;
        strm.pageData = new char[m_header.pageSize - sizeof(pageheader)];
        strm.currentPage = pgheader;
        strm.info.streamid = m_streams.size();
        strm.info.fileOffsetPage0 =  pgheader.fileOffsetThisPage;
        strm.info.streamsize = 0;
        strcpy_s(strm.info.name, sizeof(strm.info.name), name);
        strm.currentStreamPos = 0;
        strm.currentPagePos = 0;
        strm.dirty = false;

        m_streams.insert(streammap_t::value_type(strm.info.streamid, strm));

        flushStreamDirectory();
       
        streamid = strm.info.streamid;

        ++m_header.numstreams;
        writeStorageHeader();
        return SS_SUCCESS ;
    }

    int StructuredStorage::OpenStream(const char *name, int& streamid)
    {
        if (m_fd == -1)
        {
            return SS_NOT_OPENED;
        }
        streammap_t::iterator it = m_streams.begin();
        streammap_t::iterator eit = m_streams.end();
        // Make sure namne is not is use
        while (it != eit)
        {
            if (strcmp((*it).second.info.name, name) == 0)
            {
                streamid = (*it).second.info.streamid;
                return SS_SUCCESS;
            }
            ++it;
        }
        return SS_NOT_FOUND;
    }

/****************************************************************************
* Internal implementaion methods
*/
    // Load all the streamInfo's from stream 0, create a Stream and insert into map
    int StructuredStorage::loadStreams()
    {
        TT_ASSERT(m_streams.size() == 0);

        // have to manually load stream0 so Read() will work
        Stream strm;
        strm.pageData = new char[m_header.pageSize - sizeof(pageheader)];
        readPageHeader(m_header.fileOffsetFirstPageStream0, strm.currentPage);
        readPageData(strm.currentPage, strm.pageData);
        strm.currentStreamPos = 0;
        strm.currentPagePos = 0;
        strm.dirty = false;
        m_streams.insert(streammap_t::value_type(STREAM0, strm));

        int nread;
        for (int i = 0; i < m_header.numstreams; i++)
        {
            Stream strm;

            int r = Read(STREAM0, (char *)&strm.info, sizeof(streamInfo), nread);
            TT_ASSERT(r == SS_SUCCESS);
            TT_ASSERT(nread == sizeof(streamInfo));
            // STREAM0 is a little wierd. I have to mostly manually create it above, but
            // some of the data I need is in the directory stream. So for stream0, we just
            // update the streamInfo data
            if (strm.info.streamid == STREAM0)
            {
                m_streams[strm.info.streamid].info = strm.info;
            }
            else
            {
                r = readPageHeader(strm.info.fileOffsetPage0, strm.currentPage);
                if (r != SS_SUCCESS)
                {
                    return r;
                }
                strm.pageData = new char[m_header.pageSize - sizeof(pageheader)];
                r = readPageData(strm.currentPage, strm.pageData);
                if (r != SS_SUCCESS)
                {
                    return r;
                }
                strm.currentStreamPos = 0;
                strm.currentPagePos = 0;
                strm.dirty = false;
                m_streams.insert(streammap_t::value_type(strm.info.streamid, strm));
            }
        }
        return SS_SUCCESS;
    }

    // Read the storage header
    int StructuredStorage::readStorageHeader()
    {
        TT_ASSERT(m_fd > 0);
        _lseek(m_fd, 0, SEEK_SET);
        int r = _read(m_fd, &m_header, sizeof(m_header));
        if (r < 0)
        {
            TT_ASSERT(false);
            return SS_ERROR;
        }
        if (r != sizeof(fileheader))
        {
            TT_ASSERT(false);
            return SS_ERROR;
        }

        return SS_SUCCESS;
    }

    // Write the storage header
    int StructuredStorage::writeStorageHeader()
    {
        TT_ASSERT(m_fd > 0);
        _lseek(m_fd, 0, SEEK_SET);
        int r = _write(m_fd, &m_header, sizeof(m_header));
        if (r < 0)
        {
            TT_ASSERT(false);
            return SS_ERROR;
        }
        if (r != sizeof(fileheader))
        {
            TT_ASSERT(false);
            return SS_ERROR;
        }
        return SS_SUCCESS;
    }

    // Given an offset, read a page header
    int StructuredStorage::readPageHeader(int offset, pageheader& pheader)
    {
        TT_ASSERT(m_fd > 0);
        _lseek(m_fd, offset, SEEK_SET);
        int r = _read(m_fd, &pheader, sizeof(pageheader));
        if (r < 0)
        {
            TT_ASSERT(false);
            return SS_ERROR;
        }
        if (r != sizeof(pageheader))
        {
            TT_ASSERT(false);
            return SS_ERROR;
        }
        return SS_SUCCESS;
    }

    // Write a page header
    int StructuredStorage::writePageHeader(pageheader& pheader)
    {
        TT_ASSERT(m_fd > 0);
        _lseek(m_fd, pheader.fileOffsetThisPage, SEEK_SET);
        int r = _write(m_fd, &pheader, sizeof(pageheader));
        if (r < 0)
        {
            TT_ASSERT(false);
            return SS_ERROR;
        }
        if (r != sizeof(pageheader))
        {
            TT_ASSERT(false);
            return SS_ERROR;
        }
        return SS_SUCCESS;
    }

    // Read the page data for the given header
    // buf MUST point to a buffer of size PAGE_DATA_SIZE
    int StructuredStorage::readPageData(pageheader& pheader, char *buf)
    {
        TT_ASSERT(m_fd > 0);
        _lseek(m_fd, pheader.fileOffsetThisPage+sizeof(pageheader), SEEK_SET);
        int r = _read(m_fd, buf, m_pageDataSize);
        if (r < 0)
        {
            TT_ASSERT(false);
            return SS_ERROR;
        }
        if (r != m_pageDataSize)
        {
            TT_ASSERT(false);
            return SS_ERROR;
        }
        return SS_SUCCESS;
    }

    // Write the page data for the given header
    // buf MUST point to a buffer of size PAGE_DATA_SIZE
    int StructuredStorage::writePageData(pageheader& pheader, const char *buf)
    {
        TT_ASSERT(m_fd > 0);
        long ll = _lseek(m_fd, pheader.fileOffsetThisPage+sizeof(pageheader), SEEK_SET);
        cout << ll;
        int r = _write(m_fd, buf, m_pageDataSize);
        if (r < 0)
        {
            TT_ASSERT(false);
            return SS_ERROR;
        }
        if (r != m_pageDataSize)
        {
            TT_ASSERT(false);
            return SS_ERROR;
        }
        return SS_SUCCESS;
    }

    // For the given stream, read the next page, if there is one
    int StructuredStorage::loadNextPage(Stream& strm)
    {
        TT_ASSERT(m_fd > 0);
        if (strm.currentPage.fileOffsetNextPage == 0)
            return SS_NOPAGES;  // No more pages
        if (strm.dirty)
        {
            int r = writeCurrentPage(strm);
            if (r != SS_SUCCESS)
            {
                return r;
            }
        }
        int r = readPageHeader(strm.currentPage.fileOffsetNextPage, strm.currentPage);
        if (r != SS_SUCCESS)
        {
            return r;
        }

        r = readPageData(strm.currentPage, strm.pageData);
        if (r != SS_SUCCESS)
        {
            return r;
        }
        strm.currentPagePos = 0;
        return SS_SUCCESS;
    }

    // Write the current pageheader and data for the given stream
    int StructuredStorage::writeCurrentPage(Stream& strm)
    {
        TT_ASSERT(strm.currentPage.fileOffsetThisPage != 0);
        int r = writePageHeader(strm.currentPage);
        if (r != SS_SUCCESS)
            return r;
        
        r = writePageData(strm.currentPage, strm.pageData);
        if (r != SS_SUCCESS)
            return r;
        strm.dirty = false;
        return SS_SUCCESS;
    }

    // Read some bytes into buf. The amount of bytes to read must be satisfied
    // from within a single page. This is a helper function for Read()
    int StructuredStorage::readblock(Stream& strm, char *buf, int bytesToRead)
    {
        TT_ASSERT(bytesToRead <= (strm.currentPage.usedBytes-strm.currentPagePos));
        memcpy(buf, &strm.pageData[strm.currentPagePos], bytesToRead);
        strm.currentPagePos += bytesToRead;
        strm.currentStreamPos += bytesToRead;
        return SS_SUCCESS;
    }

    // Write some bytes into the current page. The amount of bytes to write must fit
    // within a single page. This is a helper function for Write()
    int StructuredStorage::writeblock(Stream& strm, const char *buf, int bytesToWrite)
    {
        TT_ASSERT((strm.currentPagePos + bytesToWrite) < m_pageDataSize);
        memcpy(&strm.pageData[strm.currentPagePos], buf, bytesToWrite);
        strm.currentPagePos += bytesToWrite;
        if (strm.currentPagePos > strm.currentPage.usedBytes)
        {
            // Increase the number of bytes used by this page
            strm.currentPage.usedBytes = strm.currentPagePos;
        }
        // Move our stream position
        strm.currentStreamPos += bytesToWrite;
        if (strm.currentStreamPos > strm.info.streamsize)
        {
            strm.info.streamsize = strm.currentStreamPos;
        }
        strm.dirty = true;
        return SS_SUCCESS;
    }

    // Allocate a page from the free list
    int StructuredStorage::allocNewPageFromFreeList(Stream& strm)
    {
        TT_ASSERT(m_header.fileOffsetFirstFreePage != 0);
        TT_ASSERT(strm.currentPage.fileOffsetNextPage == 0);

        pageheader pgheader;
        int r = readPageHeader(m_header.fileOffsetFirstFreePage, pgheader);
        if (r != SS_SUCCESS)
            return r;
        m_header.fileOffsetFirstFreePage = pgheader.fileOffsetNextPage;
        r = writeStorageHeader();
        if (r != SS_SUCCESS)
            return r;

        pgheader.usedBytes = 0;
        pgheader.streamid = strm.info.streamid;
        pgheader.fileOffsetNextPage = 0;
        r = writePageHeader(pgheader);
        if (r != SS_SUCCESS)
            return r;

        strm.currentPage.fileOffsetNextPage = pgheader.fileOffsetThisPage;
        r = writeCurrentPage(strm);
        if (r != SS_SUCCESS)
            return r;
        return SS_SUCCESS;
    }

    // Allocate a new page
    int StructuredStorage::allocNewPageFromDisk(Stream& strm)
    {
        TT_ASSERT(m_header.fileOffsetFirstFreePage == 0);
        TT_ASSERT(strm.currentPage.fileOffsetNextPage == 0);
        long pos = _lseek(m_fd, 0, SEEK_END);
        pageheader newpage;
        newpage.streamid = strm.currentPage.streamid;
        newpage.usedBytes = 0;
        newpage.fileOffsetNextPage = 0;
        newpage.fileOffsetThisPage = pos;
        int r = writePageHeader(newpage);
        if (r != SS_SUCCESS)
        {
            return r;
        }

        strm.currentPage.fileOffsetNextPage = pos;
        r = writePageHeader(strm.currentPage);
        if (r != SS_SUCCESS)
        {
            return r;
        }

        _lseek(m_fd, m_pageDataSize-1, SEEK_CUR);
        int c = 0;
        _write(m_fd, &c, 1);    // Extend the file

        return SS_SUCCESS;
    }

    // Main routine for allocating a page, select either the free list or
    // disk allocation
    int StructuredStorage::allocNewPage(Stream& strm)
    {
        int r;
        if (m_header.fileOffsetFirstFreePage != 0)
        {
            r = allocNewPageFromFreeList(strm);
        }
        else
        {
            r = allocNewPageFromDisk(strm);
        }
        if (r != SS_SUCCESS)
        {
            return r;
        }
        r = loadNextPage(strm);
        if (r != SS_SUCCESS)
        {
            return r;
        }

        return SS_SUCCESS;
    }

    int StructuredStorage::flushStreamDirectory()
    {
        TT_VERIFY(SS_SUCCESS, StreamSeek(STREAM0, 0));
        streammap_t::iterator it = m_streams.begin();
        streammap_t::iterator eit = m_streams.end();
        while (it != eit)
        {
            Stream& strm = (*it).second;
            TT_VERIFY(SS_SUCCESS, Write(STREAM0, (const char *)&strm.info, sizeof(streamInfo)));
            ++it;
        }
        return SS_SUCCESS;
    }
}

