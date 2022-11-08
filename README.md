# Karadoc


# Release notes

## 0.0.4

### Breaking changes

- Support for Python 3.6 and 3.7 has been dropped
- Removed default job import for Spark Batch: Users must replace `from karadoc.common import Job` with `from karadoc.common.run import Job`

### New features

- Added autocomplete for Karadoc, and a new command `install_autocomplete` to install it for bash

