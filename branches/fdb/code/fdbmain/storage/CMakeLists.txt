ADD_LIBRARY (fdbstorage STATIC fbtree.cpp fbufferpool.cpp fcstore.cpp ffile.cpp fkeycomp.cpp)
TARGET_LINK_LIBRARIES(fdbstorage ${GLOG_LIBRARIES} fdbio ${Boost_LIBRARIES})
