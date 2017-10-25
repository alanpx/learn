cmake_minimum_required(VERSION 3.8)
project(learn)

set(CMAKE_CXX_STANDARD 11)

include_directories("/Users/xp/devspace/opensource/leveldb/include")

set(SOURCE_FILES
        c++/src/link_list.cpp
        c++/src/main.cpp
        c++/src/tree.cpp)

add_executable(learn ${SOURCE_FILES})
target_link_libraries(learn "/Users/xp/devspace/opensource/leveldb/out-static/libleveldb.a")