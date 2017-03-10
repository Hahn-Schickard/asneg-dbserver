# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
#
# prebuild
#
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
configure_file(
    "${PROJECT_SOURCE_DIR}/OpcUaDB/Build/BuildConfig.h.in"
    "${PROJECT_BINARY_DIR}/BuildConfig.h"
)

configure_file(
    "${PROJECT_SOURCE_DIR}/OpcUaDB/linux/etc/init/OpcUaDB.conf.in"
    "${PROJECT_BINARY_DIR}/OpcUaDB.conf"
)

configure_file(
    "${PROJECT_SOURCE_DIR}/OpcUaDB/linux/etc/init.d/OpcUaDB.in"
    "${PROJECT_BINARY_DIR}/OpcUaDB"
)

configure_file(
    "${PROJECT_SOURCE_DIR}/OpcUaDB/linux/usr/lib/systemd/system/OpcUaDB.service.in"
    "${PROJECT_BINARY_DIR}/OpcUaDB.service"
)


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
#
# build 
# 
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
include_directories(
    ${OPCUASTACK_INSTALL_PREFIX}/usr/include/OpcUaStack${OPCUASTACK_VERSION_MAJOR}
    ${PROJECT_SOURCE_DIR}
    ${PROJECT_BINARY_DIR}
    ${Boost_INCLUDE_DIR}
    ${OPENSSL_INCLUDE_DIR}
)

file(
    GLOB 
    OpcUaDB_SRC 
    ${PROJECT_SOURCE_DIR}/OpcUaDB/*/*.cpp
)

add_library(
    OpcUaDB SHARED 
    ${OpcUaDB_SRC}
)

target_link_libraries(
    OpcUaDB
    ${pthread}
    ${OPCUASTACK_LIBRARY_CORE}
    ${OPCUASTACK_LIBRARY_CLIENT}
    ${Boost_LIBRARIES}
)

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
#
# install
# 
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
install(TARGETS OpcUaDB DESTINATION /usr/lib)

install(FILES ${PROJECT_SOURCE_DIR}/OpcUaDB/Config/OpcUaServer.xml DESTINATION /etc/OpcUaStack/OpcUaDB) 
install(FILES ${PROJECT_SOURCE_DIR}/OpcUaDB/Config/Opc.Ua.NodeSet.xml DESTINATION /etc/OpcUaStack/OpcUaDB)
install(FILES ${PROJECT_SOURCE_DIR}/OpcUaDB/linux/log/README DESTINATION /var/log/OpcUaStack/OpcUaDB)
install(DIRECTORY DESTINATION /var/log/OpcUaStack/OpcUaDB)

if (WIN32)
else() 
    install(PROGRAMS ${PROJECT_BINARY_DIR}/OpcUaDB DESTINATION /etc/init.d)
    install(FILES ${PROJECT_BINARY_DIR}/OpcUaDB.conf DESTINATION /etc/init)
    install(FILES ${PROJECT_BINARY_DIR}/OpcUaDB.service DESTINATION /usr/lib/systemd/system)
endif()

if (CPACK_BINARY_DEB)
    set( CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA "${CMAKE_CURRENT_SOURCE_DIR}/OpcUaDB/linux/script/postinst"
                                            "${CMAKE_CURRENT_SOURCE_DIR}/OpcUaDB/linux/script/preinst"
                                            "${CMAKE_CURRENT_SOURCE_DIR}/OpcUaDB/linux/script/prerm"
                                            "${CMAKE_CURRENT_SOURCE_DIR}/OpcUaDB/linux/script/postrm")
endif()

if (CPACK_BINARY_RPM)
    set(CPACK_RPM_POST_INSTALL_SCRIPT_FILE ${CMAKE_CURRENT_SOURCE_DIR}/OpcUaDB/linux/script/postinst)
    set(CPACK_RPM_PRE_UNINSTALL_SCRIPT_FILE ${CMAKE_CURRENT_SOURCE_DIR}/OpcUaDB/linux/script/prerm)
endif()
