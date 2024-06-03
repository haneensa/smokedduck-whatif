add_library_unity(duckdb_fade OBJECT common.cpp fade_node.cpp
        whatif_sparse.cpp
        whatif_sparse_compile.cpp
        whatif_dense.cpp
        whatif_dense_compile.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_fade>
    PARENT_SCOPE)
