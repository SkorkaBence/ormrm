#!/bin/bash

set -euo pipefail
TOP_DIR="$(cd "${0%/*}"; until [[ -f .toplevel ]]; do cd ..; done; pwd)"
set -x

GENERATOR_ARGS=()
REQUIREMENTS_FILE="${TOP_DIR}/requirements.txt"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --all)
        GENERATOR_ARGS=("--all-extras")
    ;;
    --extra)
        GENERATOR_ARGS+=("--extra")
        GENERATOR_ARGS+=("$2")
        shift
    ;;
    --requirements-file)
        REQUIREMENTS_FILE="$2"
        shift
    ;;
    --upgrade)
        GENERATOR_ARGS+=("--upgrade")
    ;;
    *)
        echo "Unknown argument: $1"
        exit 1
    ;;
  esac
  shift
done

pushd "${TOP_DIR}"

if ! command -v pip-compile &> /dev/null; then
    python3 -m pip install pip-tools
fi

python3 -m pip install \
    pip-tools \
    setuptools \
    twine

pip-compile \
    --output-file "${REQUIREMENTS_FILE}" \
    "${GENERATOR_ARGS[@]}"
python3 -m pip install -r "${REQUIREMENTS_FILE}"
popd
