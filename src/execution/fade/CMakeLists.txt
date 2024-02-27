add_library_unity(duckdb_fade OBJECT whatif_compile.cpp common.cpp predicate_search_compile.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_fade>
    PARENT_SCOPE)
