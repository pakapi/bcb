ADD_LIBRARY (fdbssb STATIC loadssb.cpp maketiny.cpp queryssb.cpp ssb.cpp dbgen.cpp runbench.cpp)
TARGET_LINK_LIBRARIES(fdbssb ${GLOG_LIBRARIES} fdbstorage)
