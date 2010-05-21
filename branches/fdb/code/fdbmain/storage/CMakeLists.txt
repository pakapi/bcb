ADD_LIBRARY (fdbstorage fbtree.cpp fbufferpool.cpp fcstore.cpp ffile.cpp fkeycomp.cpp)
TARGET_LINK_LIBRARIES(fdbstorage log4cxx fdbio ${Boost_LIBRARIES})
