cmake_minimum_required(VERSION 2.8.12...3.29)
set(TARGET_NAME postgres_scanner)
project(${TARGET_NAME})

add_definitions(-DFRONTEND=1 -D_GNU_SOURCE=1)
add_definitions(-DUSE_OPENSSL=1 -DHAVE_BIO_GET_DATA=1 -DHAVE_BIO_METH_NEW=1)
set(OPENSSL_USE_STATIC_LIBS TRUE)

find_package(OpenSSL REQUIRED)

# Try to install krb5 packages in Docker environment
if(DEFINED ENV{LINUX_CI_IN_DOCKER})
  message(STATUS "Running install_krb5.sh script...")
  execute_process(
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/install_krb5.sh
    RESULT_VARIABLE INSTALL_RESULT
    OUTPUT_VARIABLE INSTALL_OUTPUT
    ERROR_VARIABLE INSTALL_ERROR
  )
  message(STATUS "install_krb5.sh output: ${INSTALL_OUTPUT}")
  if(INSTALL_ERROR)
    message(STATUS "install_krb5.sh errors: ${INSTALL_ERROR}")
  endif()
endif()

# Try to find GSSAPI/Kerberos libraries using pkg-config first
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
  pkg_check_modules(KRB5_GSSAPI QUIET krb5-gssapi)
  pkg_check_modules(KRB5 QUIET krb5)
  pkg_check_modules(MIT_KRB5_GSSAPI QUIET mit-krb5-gssapi)
  pkg_check_modules(MIT_KRB5 QUIET mit-krb5)
endif()

# Use pkg-config results if available, but prefer static libraries for distribution
if(KRB5_GSSAPI_FOUND AND KRB5_FOUND)
  message(STATUS "Found GSSAPI via pkg-config: ${KRB5_GSSAPI_LIBRARIES}")
  add_definitions(-DHAVE_GSSAPI=1)
  set(GSSAPI_CONFIGURE_FLAG "--with-gssapi")
  set(GSSAPI_INCLUDE_DIRS ${KRB5_GSSAPI_INCLUDE_DIRS})
  
  # Try to find static libraries first for better distribution compatibility
  find_library(GSSAPI_STATIC_LIB NAMES libgssapi_krb5.a PATHS /usr/lib64 /usr/lib /usr/local/lib)
  find_library(KRB5_STATIC_LIB NAMES libkrb5.a PATHS /usr/lib64 /usr/lib /usr/local/lib)
  find_library(K5CRYPTO_STATIC_LIB NAMES libk5crypto.a PATHS /usr/lib64 /usr/lib /usr/local/lib)
  find_library(COM_ERR_STATIC_LIB NAMES libcom_err.a PATHS /usr/lib64 /usr/lib /usr/local/lib)
  find_library(KRB5SUPPORT_STATIC_LIB NAMES libkrb5support.a PATHS /usr/lib64 /usr/lib /usr/local/lib)
  
  if(GSSAPI_STATIC_LIB AND KRB5_STATIC_LIB)
    message(STATUS "Using static GSSAPI libraries for better distribution")
    set(GSSAPI_LIBRARIES ${GSSAPI_STATIC_LIB} ${KRB5_STATIC_LIB})
    if(K5CRYPTO_STATIC_LIB)
      list(APPEND GSSAPI_LIBRARIES ${K5CRYPTO_STATIC_LIB})
    endif()
    if(COM_ERR_STATIC_LIB)
      list(APPEND GSSAPI_LIBRARIES ${COM_ERR_STATIC_LIB})
    endif()
    if(KRB5SUPPORT_STATIC_LIB)
      list(APPEND GSSAPI_LIBRARIES ${KRB5SUPPORT_STATIC_LIB})
    endif()
  else()
    message(STATUS "Static libraries not found, using dynamic GSSAPI libraries")
    set(GSSAPI_LIBRARIES ${KRB5_GSSAPI_LIBRARIES} ${KRB5_LIBRARIES})
  endif()
elseif(MIT_KRB5_GSSAPI_FOUND AND MIT_KRB5_FOUND)
  message(STATUS "Found MIT GSSAPI via pkg-config: ${MIT_KRB5_GSSAPI_LIBRARIES}")
  add_definitions(-DHAVE_GSSAPI=1)
  set(GSSAPI_CONFIGURE_FLAG "--with-gssapi")
  set(GSSAPI_LIBRARIES ${MIT_KRB5_GSSAPI_LIBRARIES} ${MIT_KRB5_LIBRARIES})
  set(GSSAPI_INCLUDE_DIRS ${MIT_KRB5_GSSAPI_INCLUDE_DIRS})
else()
  # Fallback to manual library detection
  find_path(GSSAPI_INCLUDE_DIR 
    NAMES gssapi/gssapi.h gssapi.h
    PATHS /usr/include /usr/local/include /usr/include/gssapi
  )
  find_library(GSSAPI_LIBRARIES 
    NAMES gssapi_krb5 gssapi libgssapi_krb5 libgssapi
    PATHS /usr/lib64 /usr/lib /usr/local/lib
  )
  find_library(KRB5_LIBRARIES 
    NAMES krb5 libkrb5
    PATHS /usr/lib64 /usr/lib /usr/local/lib
  )

  if(GSSAPI_LIBRARIES AND KRB5_LIBRARIES)
    message(STATUS "Found GSSAPI manually: ${GSSAPI_LIBRARIES}")
    add_definitions(-DHAVE_GSSAPI=1)
    set(GSSAPI_CONFIGURE_FLAG "--with-gssapi")
    set(GSSAPI_INCLUDE_DIRS ${GSSAPI_INCLUDE_DIR})
    set(GSSAPI_LIBRARIES ${GSSAPI_LIBRARIES} ${KRB5_LIBRARIES})
  else()
    message(STATUS "GSSAPI/Kerberos not found - building without Kerberos authentication support")
    set(GSSAPI_LIBRARIES "")
    set(GSSAPI_INCLUDE_DIRS "")
    set(GSSAPI_CONFIGURE_FLAG "--without-gssapi")
  endif()
endif()

if(NOT MSVC)
  set(POSTGRES_SCANNER_EXTRA_CFLAGS
      "-Wno-pedantic -Wno-sign-compare -Wno-unused-variable")
endif()

set(CMAKE_CXX_FLAGS_DEBUG
    "${CMAKE_CXX_FLAGS_DEBUG} ${POSTGRES_SCANNER_EXTRA_CFLAGS}")
set(CMAKE_CXX_FLAGS_RELEASE
    "${CMAKE_CXX_FLAGS_RELEASE} ${POSTGRES_SCANNER_EXTRA_CFLAGS}")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO
    "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} ${POSTGRES_SCANNER_EXTRA_CFLAGS}")

set(CMAKE_C_FLAGS_DEBUG
    "${CMAKE_C_FLAGS_DEBUG} ${POSTGRES_SCANNER_EXTRA_CFLAGS} ${POSTGRES_SCANNER_EXTRA_CFLAGS}"
)
set(CMAKE_C_FLAGS_RELEASE
    "${CMAKE_C_FLAGS_RELEASE} ${POSTGRES_SCANNER_EXTRA_CFLAGS}")
set(CMAKE_C_FLAGS_RELWITHDEBINFO
    "${CMAKE_C_FLAGS_RELWITHDEBINFO} ${POSTGRES_SCANNER_EXTRA_CFLAGS}")

if(GSSAPI_INCLUDE_DIRS)
  include_directories(include postgres/src/include postgres/src/backend
                      postgres/src/interfaces/libpq ${OPENSSL_INCLUDE_DIR} ${GSSAPI_INCLUDE_DIRS})
else()
  include_directories(include postgres/src/include postgres/src/backend
                      postgres/src/interfaces/libpq ${OPENSSL_INCLUDE_DIR})
endif()

if(WIN32)
  include_directories(postgres/src/include/port/win32 postgres/src/port
                      postgres/src/include/port/win32_msvc)
endif()

set(LIBPG_SOURCES
    postgres/src/common/base64.c
    postgres/src/common/cryptohash.c
    postgres/src/common/encnames.c
    postgres/src/common/hmac.c
    postgres/src/common/ip.c
    postgres/src/common/link-canary.c
    postgres/src/common/md5.c
    postgres/src/common/md5_common.c
    postgres/src/common/saslprep.c
    postgres/src/common/scram-common.c
    postgres/src/common/sha1.c
    postgres/src/common/sha2.c
    postgres/src/common/string.c
    postgres/src/common/unicode_norm.c
    postgres/src/common/wchar.c
    postgres/src/interfaces/libpq/fe-auth-scram.c
    postgres/src/interfaces/libpq/fe-auth.c
    postgres/src/interfaces/libpq/fe-connect.c
    postgres/src/interfaces/libpq/fe-exec.c
    postgres/src/interfaces/libpq/fe-lobj.c
    postgres/src/interfaces/libpq/fe-misc.c
    postgres/src/interfaces/libpq/fe-print.c
    postgres/src/interfaces/libpq/fe-protocol3.c
    postgres/src/interfaces/libpq/fe-secure.c
    postgres/src/interfaces/libpq/fe-trace.c
    postgres/src/interfaces/libpq/legacy-pqsignal.c
    postgres/src/interfaces/libpq/libpq-events.c
    postgres/src/interfaces/libpq/pqexpbuffer.c
    postgres/src/interfaces/libpq/fe-secure-openssl.c
    postgres/src/interfaces/libpq/fe-secure-common.c
    postgres/src/port/chklocale.c
    postgres/src/port/explicit_bzero.c
    postgres/src/port/inet_net_ntop.c
    postgres/src/port/noblock.c
    postgres/src/port/pg_strong_random.c
    postgres/src/port/pgstrcasecmp.c
    postgres/src/port/snprintf.c
    postgres/src/port/strerror.c
    postgres/src/port/thread.c)

if("${CMAKE_SYSTEM_NAME}" MATCHES "Linux" OR WIN32)
  set(LIBPG_SOURCES ${LIBPG_SOURCES} postgres/src/port/strlcpy.c
                    postgres/src/port/getpeereid.c)
endif()

# Add GSSAPI source files when GSSAPI support is enabled
if(GSSAPI_LIBRARIES OR KRB5_GSSAPI_FOUND OR MIT_KRB5_GSSAPI_FOUND)
  set(LIBPG_SOURCES ${LIBPG_SOURCES} 
      postgres/src/interfaces/libpq/fe-secure-gssapi.c
      postgres/src/interfaces/libpq/fe-gssapi-common.c)
  message(STATUS "Adding GSSAPI source files to build")
endif()

if(WIN32)
  set(LIBPG_SOURCES
      ${LIBPG_SOURCES}
      postgres/src/interfaces/libpq/pthread-win32.c
      postgres/src/interfaces/libpq/win32.c
      postgres/src/port/getaddrinfo.c
      postgres/src/port/gettimeofday.c
      postgres/src/port/inet_aton.c
      postgres/src/port/open.c
      postgres/src/port/pgsleep.c
      postgres/src/port/system.c
      postgres/src/port/win32error.c
      postgres/src/port/win32ntdll.c
      postgres/src/port/win32setlocale.c
      postgres/src/port/win32stat.c)

endif()

function(PREPEND var prefix)
  set(listVar "")
  foreach(f ${ARGN})
    list(APPEND listVar "${prefix}/${f}")
  endforeach(f)
  set(${var}
      "${listVar}"
      PARENT_SCOPE)
endfunction(PREPEND)

add_subdirectory(src)

prepend(LIBPG_SOURCES_FULLPATH ${CMAKE_CURRENT_SOURCE_DIR} ${LIBPG_SOURCES})

if(NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/postgres)

  # Download the PostgreSQL source code
  message(STATUS "Downloading PostgreSQL source code")
  file(
    DOWNLOAD
    "https://github.com/postgres/postgres/archive/refs/tags/REL_15_2.tar.gz"
    ${CMAKE_CURRENT_SOURCE_DIR}/pg.tar.gz
    SHOW_PROGRESS
    EXPECTED_MD5 615ef3fa75d19c46d2dafd6c66ddb5f6
    STATUS PG_DOWNLOAD_RESULT)

  if(NOT PG_DOWNLOAD_RESULT EQUAL 0)
    file(REMOVE pg.tar.gz)
    message(FATAL_ERROR "Failed to download PostgreSQL source code")
  endif()

  # Extract the PostgreSQL source code
  message(STATUS "Extracting PostgreSQL source code")
  file(ARCHIVE_EXTRACT INPUT ${CMAKE_CURRENT_SOURCE_DIR}/pg.tar.gz DESTINATION
       ${CMAKE_CURRENT_SOURCE_DIR}/postgres_tmp)

  # Move out of root directory
  file(RENAME ${CMAKE_CURRENT_SOURCE_DIR}/postgres_tmp/postgres-REL_15_2
       ${CMAKE_CURRENT_SOURCE_DIR}/postgres)

  # Remove the tmp directory
  file(REMOVE_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/postgres_tmp)

  # Remove the downloaded tarball
  file(REMOVE ${CMAKE_CURRENT_SOURCE_DIR}/pg.tar.gz)

  # Configure the PostgreSQL source code
  message(STATUS "Configuring PostgreSQL source code")
  if(WIN32)
    # On windows, use the mkvcbuild.pl script to configure the source code
    execute_process(
      COMMAND perl mkvcbuild.pl
      RESULT_VARIABLE PG_MKVCBUILD_RESULT
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/postgres/src/tools/msvc)

    # Check if configuration was successful
    if(NOT PG_MKVCBUILD_RESULT EQUAL 0)
      file(REMOVE_RECURSE postgres)
      message(
        FATAL_ERROR "Failed to configure PostgreSQL source code for windows")
    endif()
  else()
    # On other platforms, use the configure script to configure the source code
    set(ENV{CC} gcc)
    set(ENV{CXX} g++)
    execute_process(
      COMMAND
        ./configure --without-llvm --without-icu --without-tcl --without-perl
        --without-python ${GSSAPI_CONFIGURE_FLAG} --without-pam --without-bsd-auth
        --without-ldap --without-bonjour --without-selinux --without-systemd
        --without-readline --without-libxml --without-libxslt --without-zlib
        --without-lz4 --without-openssl
      RESULT_VARIABLE PG_CONFIGURE_RESULT
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/postgres)
    # Check if configuration was successful
    if(NOT PG_CONFIGURE_RESULT EQUAL 0)
      file(REMOVE_RECURSE postgres)
      message(FATAL_ERROR "Failed to configure PostgreSQL source code")
    endif()
  endif()
  message(STATUS "Finished setting up PostgreSQL source code!")
endif()

set(PARAMETERS "-no-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${ALL_OBJECT_FILES}
                         ${LIBPG_SOURCES_FULLPATH})

if(GSSAPI_INCLUDE_DIRS)
  target_include_directories(
    ${TARGET_NAME}_loadable_extension
    PRIVATE include postgres/src/include postgres/src/backend
            postgres/src/interfaces/libpq ${OPENSSL_INCLUDE_DIR} ${GSSAPI_INCLUDE_DIRS})
else()
  target_include_directories(
    ${TARGET_NAME}_loadable_extension
    PRIVATE include postgres/src/include postgres/src/backend
            postgres/src/interfaces/libpq ${OPENSSL_INCLUDE_DIR})
endif()

if(WIN32)
  target_include_directories(
    ${TARGET_NAME}_loadable_extension
    PRIVATE postgres/src/include/port/win32 postgres/src/port
            postgres/src/include/port/win32_msvc)
endif()

if(GSSAPI_LIBRARIES)
  target_link_libraries(${TARGET_NAME}_loadable_extension ${OPENSSL_LIBRARIES} ${GSSAPI_LIBRARIES})
else()
  target_link_libraries(${TARGET_NAME}_loadable_extension ${OPENSSL_LIBRARIES})
endif()
set_property(TARGET ${TARGET_NAME}_loadable_extension PROPERTY C_STANDARD 99)

if(WIN32)
  target_link_libraries(${TARGET_NAME}_loadable_extension wsock32 ws2_32
                        wldap32 secur32 crypt32)
endif()
