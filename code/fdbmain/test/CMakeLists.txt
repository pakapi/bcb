ADD_EXECUTABLE(testmain testmain.cpp)
TARGET_LINK_LIBRARIES(testmain ${GLOG_LIBRARIES} fdbstorage fdbssb fengine boost_unit_test_framework)
