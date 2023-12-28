include_directories(src)
include_directories(../../)
include_directories(src/include)
include_directories(${pybind11_INCLUDE_DIR})
include_directories(${PYTHON_INCLUDE_DIRS})

add_subdirectory(src)

find_package(PythonLibs)
if(NOT PythonLibs_FOUND)
  return()
endif()

find_package(pybind11)
if(NOT pybind11_FOUND)
  return()
endif()
# this is used for clang-tidy checks

set(ALL_OBJECT_FILES duckdb_python.cpp ${ALL_OBJECT_FILES})

add_library(python_smokedduck ${ALL_OBJECT_FILES})
target_include_directories(python_smokedduck PRIVATE ${PYTHON_INCLUDE_DIRS})
target_link_libraries(python_smokedduck smokedduck_static ${PYTHON_LIBRARIES})
