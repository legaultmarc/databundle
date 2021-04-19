import io

from databundle.core import datasources_from_yaml_stream

yaml_example = """
sources:

  - name: dense_data_source_demo
    type: flat_file
    structure: dense
    source_parameters:
      filename: test_data/dense.csv
      delimiter: ','

"""


f = io.StringIO(yaml_example)
print(datasources_from_yaml_stream(f))
