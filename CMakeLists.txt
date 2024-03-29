cmake_minimum_required(VERSION 2.8.12)

# Set extension name here
set(TARGET_NAME scrooge)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/scrooge_extension.cpp
    src/functions/first.cpp
    src/functions/last.cpp
    src/functions/timebucket.cpp
    src/scanner/yahoo_finance.cpp
    src/functions/aliases.cpp
    src/functions/portfolio_frontier.cpp)
add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})

set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
