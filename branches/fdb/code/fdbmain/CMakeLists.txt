cmake_minimum_required(VERSION 2.6)

PROJECT(fdbmain)

#if you don't want the full compiler output, remove the following line
SET(CMAKE_VERBOSE_MAKEFILE ON)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

#add definitions, compiler switches, etc.
IF (CMAKE_BUILD_TYPE STREQUAL "DEBUG" OR CMAKE_BUILD_TYPE STREQUAL "debug" OR CMAKE_BUILD_TYPE STREQUAL "Debug")
  ADD_DEFINITIONS(-Wall -DDEBUG) # for debug. -O2 confuses debugger
ELSE (CMAKE_BUILD_TYPE STREQUAL "DEBUG" OR CMAKE_BUILD_TYPE STREQUAL "debug" OR CMAKE_BUILD_TYPE STREQUAL "Debug")
  IF(MSVC)
    # ADD_DEFINITIONS(-Wall) # MSVC doesn't like O2, causing D8016: '/O2' and '/RTC1' command-line options are incompatible
    # agggggr, -Wall is REALLY noisy in MSVC
  ELSE(MSVC)
    ADD_DEFINITIONS(-Wall -O2)
  ENDIF(MSVC)
ENDIF (CMAKE_BUILD_TYPE STREQUAL "DEBUG" OR CMAKE_BUILD_TYPE STREQUAL "debug" OR CMAKE_BUILD_TYPE STREQUAL "Debug")

find_package(Boost REQUIRED)
message ( STATUS "    Boost found in include=${Boost_INCLUDE_DIRS},lib=${Boost_LIBRARIES}")
INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIRS})
link_directories(${Boost_LIBRARIES})

find_package(Glog REQUIRED)
INCLUDE_DIRECTORIES(${GLOG_INCLUDE_DIRS})
link_directories(${GLOG_LIBRARIES})

INCLUDE_DIRECTORIES(stx-btree-0.8.3) # uses STX btree package

ADD_SUBDIRECTORY (engine)
ADD_SUBDIRECTORY (storage)
ADD_SUBDIRECTORY (ssb)
ADD_SUBDIRECTORY (io)

#list all source files here
ADD_EXECUTABLE(fdbmain main.cpp)
ADD_EXECUTABLE(testmain testmain.cpp)

# required packages
# db (BDB)
# db_cxx (BDB c++ binding)
# glog (Google logging)
# tedious to write cmake find modules. please make sure you have these installed in /usr/lib...
TARGET_LINK_LIBRARIES(fdbmain ${GLOG_LIBRARIES} fdbstorage fdbssb fengine)

# and boost test
TARGET_LINK_LIBRARIES(testmain ${GLOG_LIBRARIES} fdbstorage fdbssb fengine boost_unit_test_framework)