import os
import io
import tarfile


import pandas as pd
import h5py
import yaml


# Defines some known synonym and valid input values.
STR_TO_STRUCTURE = {
    "sparse": "sparse",
    "dense": "dense",
    "wide": "dense",
    "tidy": "long",
    "long": "long",
}


def databundle_from_yaml_stream(stream):
    conf = yaml.safe_load(stream)

    serializer = Serializer(
        conf["serializer"]["output_filename"],
        conf["serializer"].get("backend_name", "hdf5"),
        conf["serializer"].get("backend_parameters", {})
    )

    data_loaders = {
        "flat_file": SourceFlatFile
    }

    for source in conf["sources"]:
        # Get the right data loader.
        ds = data_loaders[source.pop("type")](**source)
        ds.load()
        serializer.serialize(ds)


def long_data_postprocessing(df, index_col=None, variable_col=None,
                             value_col=None):
    """Rearrange the columns to comply with the expected structure.

    Long data is always represented by: sample_col, variable_col and
    value_col.

    """
    cols = default_cols(
        df.columns,
        [index_col, variable_col, value_col]
    )

    if len(set(cols)) != len(cols):
        raise ValueError("Can't interpret data source as long dataset.")

    return df[[index_col, variable_col, value_col]]


def sparse_data_postprocessing(df, index_col=None, value_col=None):
    cols = default_cols(df.columns, [index_col, value_col])

    if len(set(cols)) != len(cols):
        raise ValueError("Can't interpret data source as sparse dataset.")

    return df[[index_col, value_col]]


def default_cols(inferred_columns, user_specified_columns):
    out = []
    for inferred_col, user_col in zip(inferred_columns, user_specified_columns):
        if user_col is None:
            out.append(inferred_columns)
        else:
            out.append(user_col)
    return out


class Serializer(object):
    """Class to persist state when serializing different data sources.

    The goal of the current package is to allow many dataframes to be loaded
    from heterogenous sources and serialized to a single file so this class
    coordinates writes to the appropriate file depending on the backend.

    """
    def __init__(self, output_filename, backend_name, backend_parameters=None):
        self.output_filename = output_filename
        self.backend_name = backend_name

        self.backend_parameters = {}
        if backend_parameters is not None:
            self.backend_parameters.update(backend_parameters)

        self.backend_init(backend_name)


    def backend_init(self, name):
        """Dispatches to the different initialization functions.

        For hdf5: removes file if already exist.
        For parquet: creates a tarball in which the different sources will
            be serialized.

        Also makes sure the backend is known.

        """
        if name == "hdf5":
            self._init_hdf5()
        elif name == "parquet":
            self._init_tar_backend()
        else:
            raise ValueError(f"Unknown backend '{name}'.")

    def _init_hdf5(self):
        if not (self.output_filename.endswith(".h5") or 
                self.output_filename.endswith(".hdf5")):
            self.output_filename += ".h5"

        try:
            with open(self.output_filename, "r"):
                pass
            os.remove(self.output_filename)
        except FileNotFoundError:
            pass


    def _init_tar_backend(self):
        if not self.output_filename.endswith(".tar"):
            self.output_filename += ".tar"

        # Create or overwrite the file.
        tar = tarfile.open(self.output_filename, "w")
        tar.close()

    def serialize(self, data_source):
        if self.backend_name == "hdf5":
            data_source._payload.to_hdf(
                self.output_filename,
                key=data_source.name,
                **self.backend_parameters
            )

        elif self.backend_name == "parquet":
            buf = io.BytesIO()
            data_source._payload.to_parquet(buf, **self.backend_parameters)
            self._write_buf_to_tar(data_source.name, buf, suffix=".parquet")

        else:
            raise ValueError(
                "Unsupported backend for serialization: '{}'."
                "".format(self.backend_name)
            )

    def _write_buf_to_tar(self, ds_name, buf, suffix=""):
        # Determine buf length.
        buf.seek(0, 2)
        buf_len = buf.tell()
        buf.seek(0)

        # Create file info.
        tar_component = tarfile.TarInfo(f"{ds_name}{suffix}")
        tar_component.size = buf_len

        # Write to the actual tar file
        with tarfile.open(self.output_filename, mode="a") as tar:
            tar.addfile(tar_component, buf)


class DataSource(object):
    def __init__(self, name, structure, structure_parameters=None,
                 source_parameters=None, backend_parameters=None):

        self.metadata = {
            "structure": STR_TO_STRUCTURE[structure.lower()]
        }
        self.name = name

        self.structure_parameters = {}
        if structure_parameters is not None:
            self.structure_parameters = structure_parameters

        self.source_parameters = {}
        if source_parameters is not None:
            self.source_parameters = source_parameters

        self._payload = None

    def load(self):
        """Load the data from the data source.

        Children are responsible for populating _payload.

        This function will do the conversions and missigness handling if
        necessary.

        """
        if self._payload is None:
            raise RuntimeError("Children need to populate payload before "
                               "calling DataSource.load().")

        if self.metadata["structure"] == "long":
            self._payload = long_data_postprocessing(
                self._payload, **self.structure_parameters
            )

        if self.metadata["structure"] == "sparse":
            self._payload = sparse_data_postprocessing(
                self._payload, **self.structure_parameters
            )


class SourcePsycopg2(DataSource):
    pass


class SourceFlatFile(DataSource):
    def __init__(self, name, structure, structure_parameters=None,
                 source_parameters=None):

        super().__init__(name, structure, structure_parameters,
                         source_parameters)

        self.metadata["filename"] = source_parameters["filename"]
        self.metadata["delimiter"] = source_parameters.get("delimiter", ",")

    def load(self):
        self._payload = pd.read_csv(self.metadata["filename"],
                                    sep=self.metadata["delimiter"])
        super().load()
