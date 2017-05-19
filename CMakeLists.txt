cmake_minimum_required(VERSION 3.6)
project(learn)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

include_directories("/Users/xp/devspace/opensource/leveldb/include")

set(SOURCE_FILES main.cpp)
add_executable(learn ${SOURCE_FILES})
target_link_libraries(learn "/Users/xp/devspace/opensource/leveldb/out-static/libleveldb.a")