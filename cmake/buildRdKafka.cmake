macro(buildrdkafka)
    add_custom_target(
        rdkafka
        COMMAND ./configure
        COMMAND make
        COMMAND make install
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/librdkafka
        VERBATIM
    )
endmacro()
