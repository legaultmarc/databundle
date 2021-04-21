import io

from databundle.core import databundle_from_yaml_stream


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

    - name: postgresql_demo
      type: postgresql
      source_parameters:
        sql: "select * from ontology_terms limit 20;"
        dbname: pfmegrnargs
        user: reader
        password: NWDMCE5xdipIjRrp
        host: hh-pgsql-public.ebi.ac.uk

"""


f = io.StringIO(yaml_example_hdf5)
databundle_from_yaml_stream(f)
