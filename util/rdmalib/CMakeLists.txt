include_directories(${PROJECT_SOURCE_DIR} ${GMINER_EXTERNAL_INCLUDES})
link_directories(${GMINER_EXTERNAL_LIBRARIES})

file(GLOB rdma-src-files
	rdmaio.cpp
    )

add_library(rdma-objs OBJECT ${rdma-src-files})