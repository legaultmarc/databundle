### What is it?

``databundle`` facilitates the aggregation of **tabular** data across variable
sources (flat files, PostgreSQL databases). It generates a single file that
contains various data sources.

The serialization format is flexible (for now either HDF5 of parquet) and
allows for efficient data loading and storage.

### Use cases

- We want to extract data from local databases and easily transport them to
  another compute server.

- We want to automatically fetch data from various online ressources (e.g. from
  public SQL databases) into a single file.

### Installation

```bash
git clone git@github.com:legaultmarc/databundle.git
pushd databundle
pip install -e .
```

### Usage

Bundling is relatively simple. First, you define the data sources and
serialization backend:

```yaml
serializer:
  output_filename: my_data_cache
  backend_name: parquet
  backend_parameters:
    engine: pyarrow

sources:
  # Data from a local SQL database
  - name: hospitalization_data
    type: postgresql
    source_parameters:
      sql: >
        select distinct sample_id::TEXT as sample_id, diagnosis_code
        from hospitalization_data
        union
        select distinct eid::TEXT, code
        from secondary_hospitalization_data
      dbname: some_psql_database
      host: hostname.domain
      user: database_username

  # Data from a delimited file
  - name: continuous_variables
    type: flat_file
    source_parameters:
      filename: test_data/dense.csv
      delimiter: ','
```

And then the `databundle my_config.yaml` command can be used to generate the
bundle.

Deserialization can be done manually or by recreating the Serde instance:

```python
import databundle.core
serde = databundle.core.ParquetSerde("my_data_cache.tar")
data = serde.deserialize()
```

### Features

**Available now**

- Supported sources:
    - Flat files (delimited files)
    - PostgreSQL databases

- Handling of missing values (done automatically by Pandas)

- Easy configuration with YAML and command-line interface

**Wishlist**

- Supported sources:
    - MySQL
    - sqlite
    - HTTP / JSON

- Backends:
    - Feather
