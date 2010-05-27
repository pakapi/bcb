#ifndef TEST_TESTMAIN_H
#define TEST_TESTMAIN_H


#ifdef WIN32
  #define NOGDI
  #include <windows.h>
  void sleepSec (int sec) {
    ::Sleep (sec * 1000);
  }
#else //WIN32
  void sleepSec (int sec) {
    ::sleep (sec);
  }
#endif //WIN32

#define TEST_DATA_FOLDER "../../data/test/"


#endif //TEST_TESTMAIN_H