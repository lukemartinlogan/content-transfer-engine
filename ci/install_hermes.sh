#!/bin/bash

set -x
set -e
set -o pipefail

INSTALL_DIR="${HOME}"
INSTALL_PREFIX="${HOME}/install"
mkdir -p "${INSTALL_PREFIX}"

mkdir build
pushd build

# Load hermes dependencies via spack
# Copy from install_deps.sh
SPACK_DIR=${INSTALL_DIR}/spack
set +x
. ${SPACK_DIR}/share/spack/setup-env.sh
set -x
spack load --only dependencies hermes

# Build hermes
# export CXXFLAGS="${CXXFLAGS} -std=c++17 -Werror -Wall -Wextra"
cmake                                                      \
    -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}               \
    -DCMAKE_BUILD_TYPE=Debug                               \
    -DBUILD_SHARED_LIBS=ON                                 \
    -DHERMES_ENABLE_COVERAGE=ON                            \
    -DHERMES_BUILD_BENCHMARKS=ON                           \
    -DHERMES_BUILD_BUFFER_POOL_VISUALIZER=OFF              \
    -DHERMES_USE_ADDRESS_SANITIZER=ON                      \
    -DHERMES_USE_THREAD_SANITIZER=OFF                      \
    -DHERMES_RPC_THALLIUM=ON                               \
    -DBUILD_TESTING=ON                                     \
    -DHERMES_ENABLE_VFD=ON                                 \
    ..
cmake --build . -- -j4

# Run unit tests
ctest -VV

popd
