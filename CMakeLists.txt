#Copyright 2018 Husky Data Lab, CUHK
#Authors: Hongzhi Chen, Miao Liu, Xiaoxi Wang


cmake_minimum_required(VERSION 3.3.0)
project(GMINER)


set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/release/)

set(CMAKE_C_COMPILER   "/data/opt/brew/bin/mpicc")
set(CMAKE_CXX_COMPILER "/data/opt/brew/bin/mpic++")
set(CMAKE_BUILD_TYPE Release CACHE STRING "set build type to release")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -m64")
set(COMMON_LINK_LIBS "-lhdfs -lpthread -libverbs -lzmq") #-ljvm 

add_definitions(-DUSE_HDFS2_)  #use hdfs v2.x.x by default
set(HDFS_INCLUDE_DIR "/data/opt/hadoop-2.6.0/include/")
set(HDFS_LIBRARY_DIR "/data/opt/hadoop-2.6.0/lib/native/")
set(ZMQ_ROOT "/data/hzchen/lib/zmq/")
set(IBVERB_ROOT "/data/xxwang/usr")

include(CMakeFiles/modules/ibverbs.cmake)
include(CMakeFiles/modules/zmq.cmake)

set(GMINER_EXTERNAL_INCLUDES  ${HDFS_INCLUDE_DIR} ${IBVERB_INCLUDE_DIR} ${ZMQ_INCLUDE_DIR})
set(GMINER_EXTERNAL_LIBRARIES ${HDFS_LIBRARY_DIR} ${IBVERB_LIBRARIES} ${ZMQ_LIBRARIES})

add_definitions(-DHAS_RDMA) # comment this line if don't have rdma device
add_subdirectory(util)
add_subdirectory(put)

# partition need different macro
add_definitions(-DPARTITION)
add_subdirectory(partition)
remove_definitions(-DPARTITION)

add_subdirectory(apps)
