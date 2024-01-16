add_library_unity(duckdb_fade OBJECT reexec.cpp why.cpp whatif.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_fade>
    PARENT_SCOPE)
