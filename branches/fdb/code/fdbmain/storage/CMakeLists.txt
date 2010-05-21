ADD_LIBRARY (fdbstorage fbtree.cpp fbufferpool.cpp fcstore.cpp ffile.cpp fkeycomp.cpp)
TARGET_LINK_LIBRARIES(fdbstorage glog fdbio ${Boost_LIBRARIES})
