from .core import databundle_from_yaml_stream, HDF5Serde, ParquetSerde


def databundle_from_yaml(filename):
    with open(filename, "r") as f:
        return databundle_from_yaml_stream(f)
