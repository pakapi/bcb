#include "fis.h"

#include <stdexcept>

#ifdef WIN32
  #define NOGDI
  #include <windows.h>
  #define INVALID_FD_VALUE INVALID_HANDLE_VALUE
#else //WIN32
  // unix/linux that supports O_DIRECT (some distribution might be not).
  #include <unistd.h>
  #include <sys/types.h>
  #include <sys/stat.h>
  #include <fcntl.h>
  #define INVALID_FD_VALUE -1 // see http://linux.die.net/man/2/open
#endif //WIN32

#include <errno.h>
#include <glog/logging.h>

namespace fdb {

DirectFileStream::DirectFileStream (const std::string &name, bool direct, bool readonly)
  : _name (name), _direct(direct), _currentLocation (0), _nextLocation (0) {

#ifdef WIN32
  _fd = ::CreateFileA (_name.c_str(),
      readonly ? GENERIC_READ : (GENERIC_READ | GENERIC_WRITE),
      readonly ? FILE_SHARE_READ : 0,
      NULL/*no inheritance*/,
      OPEN_ALWAYS,
      direct ? FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH : 0,
      NULL);
#else //WIN32
  int flags = (readonly ? O_RDONLY : O_RDWR | O_CREAT)
      | O_LARGEFILE
  #ifdef NOATIME
      | NOATIME // better for read performance if possible (linux 2.6.8-)
  #endif
      | (direct ? O_DIRECT : 0));

  _fd = ::open (_name.c_str(), flags, S_IRUSR | S_IWUSR);
#endif //WIN32

  if (_fd == INVALID_FD_VALUE) {
    LOG (ERROR) << "could not open file " << name << ". errno=" << errno;
    throw std::runtime_error("could not open file " + name + ". ");
  }
}

DirectFileStream::~DirectFileStream () {
  close ();
}

DirectFileInputStream::DirectFileInputStream (const std::string &name, bool direct) 
  : DirectFileStream (name, direct, true) {}

DirectFileOutputStream::DirectFileOutputStream (const std::string &name, bool direct) 
  : DirectFileStream (name, direct, false) {}

void DirectFileStream::close() {
  if (_fd != INVALID_FD_VALUE) {

    bool success;
#ifdef WIN32
    success = (::CloseHandle (_fd) != 0);
#else //WIN32
    success = (::close(_fd) == 0);
#endif //WIN32

    _fd = INVALID_FD_VALUE;
    if (!success) {
      LOG (ERROR) << "could not closing file " << _name << ". errno=" << errno;
      // but not throws exception.
    }
  }
}
int64_t DirectFileInputStream::read (void *buffer, int64_t size) {
  if (_nextLocation != _currentLocation) {
    VLOG (2) << "seek " << _name << " for " << _nextLocation;
#ifdef WIN32
    LARGE_INTEGER pos;
    pos.QuadPart = _nextLocation;
    bool success = ::SetFilePointerEx (_fd, pos, NULL, FILE_BEGIN) != 0;
#else //WIN32
    bool success = ::lseek64 (_fd, _nextLocation, SEEK_SET) == _nextLocation;
#endif //WIN32
    if (!success) {
      LOG (ERROR) << "could not seek file " << _name << " to offset " << _nextLocation << " . errno=" << errno;
      throw std::runtime_error("could not seek file " + _name + ". ");
    }
  }

#ifdef WIN32
  DWORD readSize = 0;
  ::ReadFile (_fd, buffer, size, &readSize, NULL);
#else //WIN32
  int64_t readSize = ::read (_fd, buffer, size);
#endif //WIN32

  _currentLocation = _nextLocation + readSize;
  _nextLocation = _currentLocation;
  if (readSize < 0) {
    LOG (ERROR) << "could not read file " << _name << " . errno=" << errno;
    throw std::runtime_error("could not read file " + _name + ". ");
  }
  return readSize;
}

int64_t DirectFileOutputStream::write (const void *buffer, int64_t size) {
  if (_nextLocation != _currentLocation) {
    VLOG (2) << "seek " << _name << " for " << _nextLocation;
#ifdef WIN32
    LARGE_INTEGER pos;
    pos.QuadPart = _nextLocation;
    bool success = ::SetFilePointerEx (_fd, pos, NULL, FILE_BEGIN) != 0;
#else //WIN32
    bool success = ::lseek64 (_fd, _nextLocation, SEEK_SET) == _nextLocation;
#endif //WIN32
    if (!success) {
      LOG (ERROR) << "could not seek file " << _name << " to offset " << _nextLocation << " . errno=" << errno;
      throw std::runtime_error("could not seek file " + _name + ". ");
    }
  }

#ifdef WIN32
  DWORD writtenSize = 0;
  ::WriteFile (_fd, buffer, size, &writtenSize, NULL);
#else //WIN32
  int64_t writtenSize = ::write (_fd, buffer, size);
#endif //WIN32
  _currentLocation = _nextLocation + writtenSize;
  _nextLocation = _currentLocation;
  if (writtenSize < 0) {
    LOG (ERROR) << "could not write file " << _name << " . errno=" << errno;
    throw std::runtime_error("could not write file " + _name + ". ");
  }
  return writtenSize;
}

void DirectFileOutputStream::sync () {
  VLOG (2) << "sync " << _name;
#ifdef WIN32
  bool success = ::FlushFileBuffers (_fd) != 0;
#else //WIN32
  bool success = ::fsync (_fd) == 0;
#endif //WIN32
  if (!success) {
    LOG (ERROR) << "could not sync file " << _name << " . errno=" << errno;
    throw std::runtime_error("could not sync " + _name + ". ");
  }
}


// see Windows: http://msdn.microsoft.com/en-us/library/cc644950(v=VS.85).aspx
// linux: http://linux.die.net/man/2/open and http://linux.die.net/man/3/posix_memalign
void* DirectFileStream::allocateMemoryForIO (size_t bufferSize, size_t alignment, bool direct) {

  if (direct) {
    void *buffer = NULL;
#ifdef WIN32
    buffer = ::VirtualAlloc(NULL, bufferSize, MEM_COMMIT, PAGE_READWRITE);
#else //WIN32
    ::posix_memalign (&buffer, alignment, bufferSize);
#endif //WIN32

    if (buffer == NULL) {
      LOG (ERROR) << "failed to call posix_memalign. errno=" << errno;
    }
    return buffer;
  } else {
    void *buffer = ::malloc (bufferSize);
    if (buffer == NULL) {
      LOG (ERROR) << "failed to allocate memory.";
    }
    return buffer;
  }
}
void DirectFileStream::deallocateMemoryForIO (bool direct, void *buffer) {
#ifdef WIN32
  if (direct) {
    ::VirtualFree(buffer, 0, MEM_RELEASE);
  } else {
    ::free (buffer);
  }
#else //WIN32
  ::free (buffer);
#endif //WIN32
}

ScopedMemoryForIO::ScopedMemoryForIO (size_t bufferSize, size_t alignment, bool direct)
  : _memory(DirectFileStream::allocateMemoryForIO (bufferSize, alignment, direct)), _direct(direct)
{}
ScopedMemoryForIO::~ScopedMemoryForIO () {
  DirectFileStream::deallocateMemoryForIO(_direct, _memory);
}


} // fdb