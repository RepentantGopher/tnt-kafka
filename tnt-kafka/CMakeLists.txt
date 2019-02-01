add_library(tntkafka SHARED tnt_kafka.c )

if (APPLE)
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} \
        -undefined suppress -flat_namespace")
endif(APPLE)

target_link_libraries(tntkafka ${RDKAFKA_LIBRARY} pthread)

set_target_properties(tntkafka PROPERTIES PREFIX "" OUTPUT_NAME "tntkafka")
install(TARGETS tntkafka LIBRARY DESTINATION ${TARANTOOL_INSTALL_LIBDIR}/tnt-kafka)
install(FILES init.lua DESTINATION ${TARANTOOL_INSTALL_LUADIR}/tnt-kafka)
