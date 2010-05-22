#ifndef IO_FIS_H
#define IO_FIS_H

#include <stdint.h>
#include <string>
#include <cstdlib>

namespace fdb {

class DirectFileStream {
public:
  DirectFileStream (const std::string &name, int flags);
  virtual ~DirectFileStream ();

  void close();

  int64_t getCurrentLocation () const { return _currentLocation; }
  int64_t getNextLocation () const { return _nextLocation; }
  void setNextLocation (int64_t nextLocation) { _nextLocation = nextLocation; }

  // all buffers used for these classes should be obtained and released by these methods.
  // when direct=true, these methods use linux's appropriate methods to align buffer for O_DIRECT.
  // note that bufferSize has to be a multiply of linux's page size. Google posix_memalign for details.
  static void* allocateMemoryForIO (size_t bufferSize, size_t alignment, bool direct);
  static void deallocateMemoryForIO (bool direct, void *buffer);

protected:
  void init (const std::string &name, int flags);

  std::string _name;
  int _fd;
  int64_t _currentLocation;
  int64_t _nextLocation;
};

// works as scoped_array<char> for O_DIRECT.
// like boost::scoped_array, this prohibits copying and assignments.
class ScopedMemoryForIO {
private: // these are prohibited and have no implementation
  ScopedMemoryForIO(const ScopedMemoryForIO &); 
  ScopedMemoryForIO& operator==(const ScopedMemoryForIO &);
  void operator==(const ScopedMemoryForIO &) const;
  void operator!=(const ScopedMemoryForIO &) const;

  void *_memory;
  bool _direct;
public:
  ScopedMemoryForIO (size_t bufferSize, size_t alignment, bool direct);
  ~ScopedMemoryForIO ();
  bool direct() {return _direct;}
  void* get() {return _memory;}
};

/** same as std::ifstream except this supports DIRECT_IO. */
class DirectFileInputStream : public DirectFileStream {
public:
  DirectFileInputStream (const std::string &name, bool direct);
  DirectFileInputStream (const std::string &name, int flags);
  virtual ~DirectFileInputStream () {};

  int64_t read (void *buffer, int64_t size);
};

/** same as std::ifstream except this supports DIRECT_IO. */
class DirectFileOutputStream : public DirectFileStream {
public:
  DirectFileOutputStream (const std::string &name, bool direct);
  DirectFileOutputStream (const std::string &name, int flags);
  virtual ~DirectFileOutputStream () {};

  int64_t write (const void *buffer, int64_t size);
  void sync ();
};

} // fdb

#endif // IO_FIS_H
