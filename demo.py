import io

from databundle.core import databundle_from_yaml_stream

yaml_example_parquet = """
serializer:
    output_filename: my_databundle_parquet_fastparquet
    backend_name: parquet
    backend_parameters:
        engine: fastparquet
        compression: GZIP

sources:
    - name: dense_data_source_demo
      type: flat_file
      structure: dense
      source_parameters:
          filename: test_data/dense.csv
          delimiter: ','

"""


f = io.StringIO(yaml_example_parquet)
databundle_from_yaml_stream(f)

###

yaml_example_parquet_pyarrow = """
serializer:
    output_filename: my_databundle_parquet_pyarrow
    backend_name: parquet
    backend_parameters:
        engine: pyarrow

sources:
    - name: dense_data_source_demo
      type: flat_file
      structure: dense
      source_parameters:
          filename: test_data/dense.csv
          delimiter: ','

"""

f = io.StringIO(yaml_example_parquet_pyarrow)
databundle_from_yaml_stream(f)

###

yaml_example_hdf5 = """
serializer:
    output_filename: my_databundle
    backend_name: hdf5

sources:
    - name: dense_data_source_demo
      type: flat_file
      structure: dense
      source_parameters:
          filename: test_data/dense.csv
          delimiter: ','

"""


f = io.StringIO(yaml_example_hdf5)
databundle_from_yaml_stream(f)
