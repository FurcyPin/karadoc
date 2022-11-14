# Karadoc


# Release notes

## 0.1.0

### Breaking changes

- Moved all spark-related code into a karadoc.spark module. Users must replace `from karadoc.common.run import Job`
  and `from karadoc.common import Job` with `from karadoc.spark.batch import Job`

## 0.0.4

### Breaking changes

- Support for Python 3.6 and 3.7 has been dropped
- Removed default job import for Spark Batch: Users must replace `from karadoc.common import Job` with `from karadoc.common.run import Job`

### New features

- Added autocomplete for Karadoc, and a new command `install_autocomplete` to install it for bash

