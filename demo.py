import io

from databundle.core import databundle_from_yaml_stream

yaml_example_parquet = """
serializer:
    output_filename: my_databundle
    backend_name: parquet

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
