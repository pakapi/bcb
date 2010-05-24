#ifndef UTIL_STOPWATCH_H
#define UTIL_STOPWATCH_H

#ifdef _MSC_VER
#define NOGDI //otherwise wingdi.h defines ERROR=0, conflicting with glog
#include <windows.h>
#else
#include <sys/time.h>
#endif //_MSC_VER

class StopWatch {
public:
  StopWatch() {
    init();
  }

  void init() {
#ifdef _MSC_VER
    ::QueryPerformanceCounter (&starttime_);
#else
    gettimeofday(&starttime_, NULL);
#endif //_MSC_VER
  }

  void stop() {
#ifdef _MSC_VER
    ::QueryPerformanceCounter (&stoptime_);
#else
    gettimeofday(&stoptime_, NULL);
#endif //_MSC_VER
  }

  long long getElapsed() const {
#ifdef _MSC_VER
    LARGE_INTEGER tps;
    QueryPerformanceFrequency(&tps);
    return (stoptime_.QuadPart - starttime_.QuadPart) * 1000000 / tps.QuadPart;
#else
    timeval resulttime;
    timersub(&stoptime_, &starttime_, &resulttime);
    long long wallClock = resulttime.tv_sec * 1000000 + resulttime.tv_usec;
    return wallClock;
#endif //_MSC_VER
  }

#ifdef _MSC_VER
  LARGE_INTEGER
#else
  timeval
#endif //_MSC_VER
    starttime_, stoptime_;
};

#endif// UTIL_STOPWATCH_H
