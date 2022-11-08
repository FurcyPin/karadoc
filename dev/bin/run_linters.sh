#!/bin/bash
set -e

poetry run black .
poetry run isort karadoc tests
poetry run flake8 karadoc tests
poetry run mypy karadoc

