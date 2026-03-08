#!/bin/bash

set -euo pipefail
TOP_DIR="$(cd "${0%/*}"; until [[ -f .toplevel ]]; do cd ..; done; pwd)"
set -x

OUT_DIR="${TOP_DIR}/dist"
PUBLISH_TARGET="${1:-internal}"

if [[ "${PUBLISH_TARGET}" = "internal" ]]; then
    if [[ -n "${CI_API_V4_URL:-}" ]] && [[ -n "${CI_PROJECT_ID:-}" ]] && [[ -n "${CI_JOB_TOKEN:-}" ]]; then
        twine upload \
            --repository-url "${CI_API_V4_URL}/projects/${CI_PROJECT_ID}/packages/pypi" \
            --username "gitlab-ci-token" \
            --password "${CI_JOB_TOKEN}" \
            "${OUT_DIR}/*"
    fi
elif [[ "${PUBLISH_TARGET}" = "pypi" ]]; then
    if [[ -n "${PYPI_TOKEN}" ]]; then
        twine upload \
            --username "__token__" \
            --password "${PYPI_TOKEN}" \
            "${OUT_DIR}/*"
    fi
fi
