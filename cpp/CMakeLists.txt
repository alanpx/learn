cmake_minimum_required(VERSION 3.8)
project(learn)

set(CMAKE_CXX_STANDARD 11)

include_directories("/Users/xp/devspace/opensource/leveldb/include")
include_directories("/usr/local/BerkeleyDB.6.2/include/")

set(SOURCE_FILES
    link_list.cpp
    main.cpp
    tree.cpp)

add_executable(learn ${SOURCE_FILES})
target_link_libraries(learn "/Users/xp/devspace/opensource/leveldb/out-static/libleveldb.a")
target_link_libraries(learn "/usr/local/BerkeleyDB.6.2/lib/libdb_cxx.a")
