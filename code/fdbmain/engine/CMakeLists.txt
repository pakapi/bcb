ADD_LIBRARY (fengine STATIC fengine.cpp ffamily.cpp)
TARGET_LINK_LIBRARIES(fengine ${GLOG_LIBRARIES} fdbstorage)
