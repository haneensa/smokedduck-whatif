import os

duckdb_whl = "duckdb-0.1.dev28329+gb9c20ef.d20230828-cp39-cp39-linux_x86_64.whl"
duckdb_path = "/home/haneenmo/duckdb_lineage/tools"
duckdb_wheel = f"{duckdb_path}/pythonpkg/dist/{duckdb_whl}"

toml_content = f"""
[tool.poetry.dependencies]
python = "^3.8"
duckdb = {{path = "{duckdb_wheel}"}}
numpy = "^1.21.0"
# Other dependencies
"""

with open("pyproject.toml", "a") as toml_file:
    toml_file.write(toml_content)
