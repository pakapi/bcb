cmake_minimum_required(VERSION 2.6)

PROJECT(fdbmain)

#if you don't want the full compiler output, remove the following line
SET(CMAKE_VERBOSE_MAKEFILE ON)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

#add definitions, compiler switches, etc.
IF (CMAKE_BUILD_TYPE STREQUAL "DEBUG" OR CMAKE_BUILD_TYPE STREQUAL "debug" OR CMAKE_BUILD_TYPE STREQUAL "Debug")
  ADD_DEFINITIONS(-Wall -DDEBUG) # for debug. -O2 confuses debugger
ELSE (CMAKE_BUILD_TYPE STREQUAL "DEBUG" OR CMAKE_BUILD_TYPE STREQUAL "debug" OR CMAKE_BUILD_TYPE STREQUAL "Debug")
  ADD_DEFINITIONS(-Wall -O2)
ENDIF (CMAKE_BUILD_TYPE STREQUAL "DEBUG" OR CMAKE_BUILD_TYPE STREQUAL "debug" OR CMAKE_BUILD_TYPE STREQUAL "Debug")

find_package(Boost REQUIRED filesystem)
message ( STATUS "    Boost found in include=${Boost_INCLUDE_DIRS},lib=${Boost_LIBRARIES}")
INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIRS})
link_directories(${Boost_LIBRARIES})

find_package(Log4cxx REQUIRED)
message ( STATUS "    Log4cxx found in include=${LOG4CXX_INCLUDE_DIRS},lib=${LOG4CXX_LIBRARIES}")
INCLUDE_DIRECTORIES(${LOG4CXX_INCLUDE_DIRS})
link_directories(${LOG4CXX_LIBRARIES})

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
# log4cxx
# tedious to write cmake find modules. please make sure you have these installed in /usr/lib...
TARGET_LINK_LIBRARIES(fdbmain log4cxx fdbstorage fdbssb fengine)

# and boost test
TARGET_LINK_LIBRARIES(testmain log4cxx fdbstorage fdbssb fengine boost_unit_test_framework)
