#!/bin/bash

set -euo pipefail
TOP_DIR="$(cd "${0%/*}"; until [[ -f .toplevel ]]; do cd ..; done; pwd)"
set -x

TMP_DIR="$(mktemp --directory --tmpdir build-XXXXXXXX)"

if [[ "${CI_COMMIT_REF_NAME:-}" == "${CI_DEFAULT_BRANCH:-master}" ]]; then
    TARGET_VERSION="$(git describe --tags | sed -e 's/^v*//' -e 's/-g[a-z0-9]*$//g' -e 's/-/./')"
    if [[ "${TARGET_VERSION}" =~ ^[0-9]+\.[0-9]+$ ]]; then
        TARGET_VERSION="${TARGET_VERSION}.0"
    fi
else
    TARGET_VERSION="$(git describe --tags | sed -e 's/^v*//' -e 's/-g/+g/g' -e 's/-/.dev/')"
fi

cleanup() {
    rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

TARGET_DIR="${TMP_DIR}/ormrm-${TARGET_VERSION}"
OUT_DIR="${TOP_DIR}/dist"

rm -rf "${OUT_DIR}"
mkdir -p "${TARGET_DIR}" "${OUT_DIR}"

rsync -rt \
    --exclude "__pycache__" \
    --exclude "__tests__" \
    "${TOP_DIR}/ormrm" \
    "${TOP_DIR}/pyproject.toml" \
    "${TOP_DIR}/README.md" \
    "${TOP_DIR}/LICENSE" \
    "${TARGET_DIR}/"

sed -i "${TARGET_DIR}/pyproject.toml" \
    -e "s|version = .*|version = \"${TARGET_VERSION}\"|g"

pushd "${TARGET_DIR}"
pip-compile ./pyproject.toml -v
python3 -m build --outdir "${OUT_DIR}"
popd

twine check "${OUT_DIR}/*"
