find_path(RDKAFKA_ROOT_DIR
        NAMES include/librdkafka/rdkafka.h
        )

find_path(RDKAFKA_INCLUDE_DIR
        NAMES librdkafka/rdkafka.h
        HINTS ${RDKAFKA_ROOT_DIR}/include
        )
find_library(RDKAFKA_LIBRARY
        NAMES ${CMAKE_SHARED_LIBRARY_PREFIX}rdkafka${CMAKE_SHARED_LIBRARY_SUFFIX} rdkafka
        HINTS ${RDKAFKA_ROOT_DIR}/lib
        )

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(RDKAFKA DEFAULT_MSG
        RDKAFKA_LIBRARY
        RDKAFKA_INCLUDE_DIR
        )

mark_as_advanced(
        RDKAFKA_ROOT_DIR
        RDKAFKA_INCLUDE_DIR
        RDKAFKA_LIBRARY
)
