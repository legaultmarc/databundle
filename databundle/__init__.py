from .core import (
    databundle_from_yaml_stream,
    HDF5Serde,
    ParquetSerde,
    get_serde_from_filename
)


def databundle_from_yaml(filename):
    with open(filename, "r") as f:
        return databundle_from_yaml_stream(f)
