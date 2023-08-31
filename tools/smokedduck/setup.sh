# 1. copy files from pythonpkg
# 2. update setup.py
#!/bin/bash

# Source and destination folders
source_folder="../pythonpkg"
destination_folder="."

rm -f setup.py
# Copy contents of source folder to destination folder
cp -r  --no-clobber  "$source_folder"/* "$destination_folder"

# Customized setup block content
custom_setup=$(cat << 'END_HEREDOC'
    name='smokedduck',
    version='1.0',
    description='Fine-grained Lineage Enabled DuckDB',
    long_description=long_description,
    long_description_content_type='text/markdown',
    keywords = 'Lineage DuckDB Database SQL OLAP',
    license='MIT',
    data_files = data_files,
    packages=[
        'pyduckdb',
        'duckdb-stubs',
        lib_name,
        'smokedduck'
    ],
    install_requires=[
        'pandas<=2.0.3',
        'numpy>=1.14'
    ],
    include_package_data=True,
    setup_requires=setup_requires + ["setuptools_scm<7.0.0", 'pybind11>=2.6.0'],
    tests_require=['pytest'],
    ext_modules = [libduckdb],
    maintainer = "Haneen Mohammed, Charlie Summers",
    maintainer_email = "smokedduckdb@gmail.com",
    cmdclass={"build_ext": build_ext}
)
END_HEREDOC
)

# Path to the setup.py file
setup_py_file="setup.py"

# Extract the lines before setup( and save them to a temp file
sed '/^setup(/q' "$setup_py_file" > temp_setup.py

# Append the custom content to the temp file if setup( was not found
if grep -q '^setup(' "$setup_py_file"; then
    echo "$custom_setup" >> temp_setup.py
fi

# Move the temp file back to the setup.py file
mv temp_setup.py "$setup_py_file"

custom_readme=$(cat << 'END_HEREDOC'
from pathlib import Path
this_directory = Path(__file__).parent
long_description = (this_directory / "SD_README.md").read_text()
END_HEREDOC
)

echo "$custom_readme" | cat - "$setup_py_file" > tempreadme && mv tempreadme "$setup_py_file"

# Define the start marker for the section to remove
start_marker="## SQL Reference"

readme_file="README.md"
long_description_file="SD_README.md"
# Read the contents of the readme.md file
contents=`cat "$readme_file"`

# Use 'sed' to remove the section and everything after it
updated_contents=$(echo "$contents" | sed -e "/$start_marker/,\$d")

# Write the updated contents back to the readme.md file
echo "$updated_contents" > "$long_description_file"
