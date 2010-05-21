# Try to find the libglog libraries
# Once done this will define :
#
# Glog_FOUND - system has libglog
# Glog_INCLUDE_DIRS - the libglog include directory
# Glog_LIBRARIES - libglog library

FIND_PATH(GLOG_INCLUDE_DIRS logging.h PATHS /include/glog /usr/include/glog /usr/local/include/glog )
FIND_LIBRARY(GLOG_LIBRARIES NAMES glog PATHS /lib /usr/lib /usr/local/lib )

IF(GLOG_INCLUDE_DIRS AND GLOG_LIBRARIES)
  SET(Glog_FOUND 1)
  #remove last /glog string
  STRING(REGEX REPLACE "/glog" "" GLOG_INCLUDE_DIRS_SUP_LEVEL ${GLOG_INCLUDE_DIRS})
  SET (GLOG_INCLUDE_DIRS ${GLOG_INCLUDE_DIRS_SUP_LEVEL} ${GLOG_INCLUDE_DIRS} )
  if(NOT Glog_FIND_QUIETLY)
   message (STATUS "    glog found in include=${GLOG_INCLUDE_DIRS},lib=${GLOG_LIBRARIES}")
  endif(NOT Glog_FIND_QUIETLY)
ELSE(GLOG_INCLUDE_DIRS AND GLOG_LIBRARIES)
  SET(Glog_FOUND 0 CACHE BOOL "Not found glog library")
  message(STATUS "NOT Found glog")
ENDIF(GLOG_INCLUDE_DIRS AND GLOG_LIBRARIES)

MARK_AS_ADVANCED(GLOG_INCLUDE_DIRS GLOG_LIBRARIES)
