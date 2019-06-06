cmake_minimum_required(VERSION 2.8 FATAL_ERROR)

project(kafka C)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})

# Set CFLAGS
set(MY_C_FLAGS "-Wall -Wextra -Werror -std=gnu11 -fno-strict-aliasing -Wno-deprecated-declarations")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${MY_C_FLAGS}")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} ${MY_C_FLAGS} -ggdb3")

set(TARANTOOL_FIND_REQUIRED ON)
find_package(Tarantool)
# Oracle Connector requires fiber_cond API added in 1.7.4-291
string(REPLACE "-" "." tntver ${TARANTOOL_VERSION})
if(${tntver} VERSION_LESS 1.7.4.291)
    message(FATAL_ERROR "Tarantool >= 1.7.4-291 is required")
endif()

if("${STATIC_BUILD}" OR $ENV{STATIC_BUILD})
    include(buildRdKafka)
    buildrdkafka()
else()
    set(RDKAFKA_FIND_REQUIRED ON)
    find_package(RdKafka)
endif()

include_directories(${TARANTOOL_INCLUDE_DIRS})

add_subdirectory(kafka)
