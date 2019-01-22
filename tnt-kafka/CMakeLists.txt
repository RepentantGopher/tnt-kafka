add_library(tnt_kafka SHARED tnt_kafka.c )

if (APPLE)
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} \
        -undefined suppress -flat_namespace")
endif(APPLE)

target_link_libraries(tnt_kafka ${RDKAFKA_LIBRARIES} pthread)

set_target_properties(tnt_kafka PROPERTIES PREFIX "" OUTPUT_NAME "tnt_kafka")
install(TARGETS tnt_kafka LIBRARY DESTINATION ${TARANTOOL_INSTALL_LIBDIR}/tnt_kafka)
install(FILES init.lua DESTINATION ${TARANTOOL_INSTALL_LUADIR}/tnt_kafka)
