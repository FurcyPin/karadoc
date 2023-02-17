# Karadoc


# Release notes

## 0.1.0 (Not released yet)

### Breaking changes

- Moved all spark-related code into a `karadoc.spark` module.
  Users must replace `from karadoc.common.run import Job` and `from karadoc.common import Job` 
  with `from karadoc.spark.batch import Job`
- Built-in connectors have been moved from `karadoc.connectors` to `karadoc.spark.connectors` 
- The default configuration parameter `model_dir` has been changed from `"model"` to `"models"`

### New features

- `show_graph` command can now be called without the `--tables` option. When done so, it will
  display the dependency graph for all models.

## 0.0.4

### Breaking changes

- Support for Python 3.6 and 3.7 has been dropped
- Removed default job import for Spark Batch: Users must replace `from karadoc.common import Job` with `from karadoc.common.run import Job`

### New features

- Added autocomplete for Karadoc, and a new command `install_autocomplete` to install it for bash

