#!/bin/bash
set -e -u -x

DOCKER_IMAGE=quay.io/pypa/manylinux2014_x86_64
PLAT=manylinux2014_x86_64

cd /io/tools/smokedduck/

function repair_wheel {
    wheel="$1"
    if ! auditwheel show "$wheel"; then
        echo "Skipping non-platform wheel $wheel"
    else
        auditwheel repair "$wheel" --plat "$PLAT" -w /io/wheelhouse/
    fi
}


# Compile wheels
PYBIN=/opt/python/cp39-cp39/bin
BUILD_LINEAGE=1 "${PYBIN}/pip" wheel . --no-deps -w wheelhouse/

PYBIN=/opt/python/cp310-cp310/bin
BUILD_LINEAGE=1 "${PYBIN}/pip" wheel . --no-deps -w wheelhouse/

PYBIN=/opt/python/cp311-cp311/bin
BUILD_LINEAGE=1 "${PYBIN}/pip" wheel . --no-deps -w wheelhouse/

# Bundle external shared libraries into the wheels
for whl in wheelhouse/*.whl; do
    repair_wheel "$whl"
done
