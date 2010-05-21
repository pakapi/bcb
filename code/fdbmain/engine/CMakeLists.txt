ADD_LIBRARY (fengine fengine.cpp ffamily.cpp)
TARGET_LINK_LIBRARIES(fengine glog fdbstorage)
