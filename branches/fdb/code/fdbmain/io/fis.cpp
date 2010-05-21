#include "fis.h"

#include <stdexcept>

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include <glog/logging.h>

namespace fdb {

DirectFileStream::DirectFileStream (const std::string &name, int flags) {
  init(name, flags);
}
void DirectFileStream::init (const std::string &name, int flags) {
  _name = name;
  _fd = ::open (name.c_str(), flags, S_IRUSR | S_IWUSR);
  if (_fd < 0) {
    LOG (ERROR) << "could not open file " << name << ". errno=" << errno;
    throw std::runtime_error("could not open file " + name + ". ");
  }
  _currentLocation = 0;
  _nextLocation = 0;
}

DirectFileStream::~DirectFileStream () {
  close ();
}

DirectFileInputStream::DirectFileInputStream (const std::string &name, bool direct) 
  : DirectFileStream (name, O_RDONLY | O_LARGEFILE |
#ifdef NOATIME
    NOATIME | // better for read performance if possible (linux 2.6.8-)
#endif
    (direct ? O_DIRECT : 0)) {
}
DirectFileInputStream::DirectFileInputStream (const std::string &name, int flags)
  : DirectFileStream (name, flags) {
}

DirectFileOutputStream::DirectFileOutputStream (const std::string &name, bool direct) 
  : DirectFileStream (name, /*O_WRONLY*/ O_RDWR | O_LARGEFILE | O_CREAT | (direct ? O_DIRECT : 0)) {
}
DirectFileOutputStream::DirectFileOutputStream (const std::string &name, int flags)
  : DirectFileStream (name, flags) {
}

void DirectFileStream::close() {
  if (_fd >= 0) {
    int ret = ::close(_fd);
    _fd = -1;
    if (ret != 0) {
      LOG (ERROR) << "could not closing file " << _name << ". errno=" << errno;
      // but not throws exception.
    }
  }
}
int64_t DirectFileInputStream::read (void *buffer, int64_t size) {
  if (_nextLocation != _currentLocation) {
    VLOG (2) << "seek " << _name << " for " << _nextLocation;
    int64_t ret = ::lseek64 (_fd, _nextLocation, SEEK_SET);
    if (ret != _nextLocation) {
      LOG (ERROR) << "could not seek file " << _name << " to offset " << _nextLocation << " . errno=" << errno;
      throw std::runtime_error("could not seek file " + _name + ". ");
    }
  }

  int64_t readSize = ::read (_fd, buffer, size);
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
    int64_t ret = ::lseek64 (_fd, _nextLocation, SEEK_SET);
    if (ret != _nextLocation) {
      LOG (ERROR) << "could not seek file " << _name << " to offset " << _nextLocation << " . errno=" << errno;
      throw std::runtime_error("could not seek file " + _name + ". ");
    }
  }

  int64_t writtenSize = ::write (_fd, buffer, size);
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
  int ret = ::fsync (_fd);
  if (ret < 0) {
    LOG (ERROR) << "could not sync file " << _name << " . errno=" << errno;
    throw std::runtime_error("could not sync " + _name + ". ");
  }
}

void* DirectFileStream::allocateMemoryForIO (size_t bufferSize, size_t alignment, bool direct) {
  if (direct) {
    void *buffer = NULL;
    int ret = ::posix_memalign (&buffer, alignment, bufferSize);
    if (ret != 0) {
      LOG (ERROR) << "failed to call posix_memalign. ret=" << ret;
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
  ::free (buffer);
}

ScopedMemoryForIO::ScopedMemoryForIO (size_t bufferSize, size_t alignment, bool direct)
  : _memory(DirectFileStream::allocateMemoryForIO (bufferSize, alignment, direct)), _direct(direct)
{}
ScopedMemoryForIO::~ScopedMemoryForIO () {
  DirectFileStream::deallocateMemoryForIO(_direct, _memory);
}


} // fdb