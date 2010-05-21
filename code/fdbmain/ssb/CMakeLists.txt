ADD_LIBRARY (fdbssb loadssb.cpp maketiny.cpp queryssb.cpp ssb.cpp dbgen.cpp runbench.cpp)
TARGET_LINK_LIBRARIES(fdbssb log4cxx fdbstorage)
