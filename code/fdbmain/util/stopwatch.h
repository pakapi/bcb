#ifndef UTIL_STOPWATCH_H
#define UTIL_STOPWATCH_H

#include <sys/time.h>

class StopWatch {
public:
  StopWatch() {
    init();
  }
  void init() {
    gettimeofday(&starttime_, NULL);
  }
  void stop() {
    gettimeofday(&stoptime_, NULL);
  }
  long long getElapsed() const {
    timeval resulttime;
    timersub(&stoptime_, &starttime_, &resulttime);
    long long wallClock = resulttime.tv_sec * 1000000 + resulttime.tv_usec;
    return wallClock;
  }
  timeval starttime_, stoptime_;
};

#endif// UTIL_STOPWATCH_H
