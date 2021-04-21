import os
import io
import tarfile
import tempfile


import pandas as pd
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

    serdes = {
        "hdf5": HDF5Serde,
        "parquet": ParquetSerde
    }

    serde_conf = conf["serializer"]
    serde = serdes[serde_conf.get("backend_name", "hdf5")](
        output_filename=serde_conf["output_filename"],
        backend_parameters=serde_conf.get("backend_parameters", {})
    )

    # Backend need to be initialized for serializing.
    serde.backend_init()

    data_loaders = {
        "flat_file": SourceFlatFile,
        "postgresql": SourcePsycopg2
    }

    for source in conf["sources"]:
        # Get the right data loader.
        ds = data_loaders[source.pop("type")](**source)
        ds.load()
        serde.serialize(ds)


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

    return df[cols]


def sparse_data_postprocessing(df, index_col=None, value_col=None):
    cols = default_cols(df.columns, [index_col, value_col])

    if len(set(cols)) != len(cols):
        raise ValueError("Can't interpret data source as sparse dataset.")

    return df[cols]


def default_cols(inferred_columns, user_specified_columns):
    """Allows customization of column names for expected columns for data
       formats with semantics (sparse or long)."""
    out = []
    for inferred_col, user_col in zip(inferred_columns, user_specified_columns):
        if user_col is None:
            out.append(inferred_col)
        else:
            out.append(user_col)
    return out


class Serde(object):
    """Class to persist state when serializing different data sources.

    The goal of the current package is to allow many dataframes to be loaded
    from heterogenous sources and serialized to a single file so this class
    coordinates writes to the appropriate file depending on the backend.

    """
    def __init__(self, output_filename, backend_parameters=None):
        self.output_filename = output_filename

        self.backend_parameters = {}
        if backend_parameters is not None:
            self.backend_parameters.update(backend_parameters)

    def backend_init(self):
        raise NotImplementedError()

    def serialize(self, data_source):
        raise NotImplementedError()

    def deserialize(self, data_source_name=None):
        raise NotImplementedError()

    def _init_tar_backend(self):
        """Utility function for serializers that use tarballs to aggregate
        files."""
        if not self.output_filename.endswith(".tar"):
            self.output_filename += ".tar"

        # Create or overwrite the file.
        tar = tarfile.open(self.output_filename, "w")
        tar.close()

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


class HDF5Serde(Serde):
    def backend_init(self):
        if not (self.output_filename.endswith(".h5") or
                self.output_filename.endswith(".hdf5")):
            self.output_filename += ".h5"

        try:
            with open(self.output_filename, "r"):
                pass
            os.remove(self.output_filename)
        except FileNotFoundError:
            pass

    def serialize(self, data_source):
        data_source._payload.to_hdf(
            self.output_filename,
            key=data_source.name,
            **self.backend_parameters
        )


    def deserialize(self, data_source_name=None):
        import h5py
        if data_source_name is None:
            with h5py.File(self.output_filename, "r") as f:
                keys = list(f.keys())

            # Deserialize everything.
            out = {}
            for k in keys:
                out[k] = pd.read_hdf(self.output_filename, key=k)

            return out

        return pd.read_hdf(self.output_filename, key=data_source_name)


class ParquetSerde(Serde):
    def backend_init(self):
        self._init_tar_backend()

    def _infer_parquet_engine(self):
        import pandas.io.parquet
        engine_str = self.backend_parameters.get("engine", "auto")
        engine_impl = pandas.io.parquet.get_engine(engine_str)

        if type(engine_impl) is pandas.io.parquet.PyArrowImpl:
            return "pyarrow"

        if type(engine_impl) is pandas.io.parquet.FastParquetImpl:
            return "fastparquet"

        raise ValueError("Could not infer parquet engine.")


    def serialize(self, data_source):
        engine = self._infer_parquet_engine()

        if engine == "pyarrow":
            buf = io.BytesIO(data_source._payload.to_parquet(
                path=None, **self.backend_parameters
            ))
            self._write_buf_to_tar(data_source.name, buf,
                                   suffix=".parquet")
        else:
            # fastparquet does not support in memory serialization, so we
            # need to create a temporary file.
            with tempfile.NamedTemporaryFile("w+b") as f:
                data_source._payload.to_parquet(path=f.name,
                                                **self.backend_parameters)
                self._write_buf_to_tar(data_source.name, f,
                                       suffix=".parquet")

    def deserialize(self, data_source_name=None):
        with tarfile.open(self.output_filename, mode="r") as tar:
            if data_source_name is None:
                out = {}
                names = tar.getnames()
                for key in names:
                    data = tar.extractfile(key)
                    out[key] = pd.read_parquet(data)

                return out

            # A suffix is automatically added to data source names for
            # serialization so we add it here if needed.
            if not data_source_name.endswith(".parquet"):
                data_source_name += ".parquet"

            return pd.read_parquet(tar.extractfile(data_source_name))


class DataSource(object):
    def __init__(self, name, structure=None, structure_parameters=None,
                 source_parameters=None, backend_parameters=None):

        self.metadata = {
            "structure": STR_TO_STRUCTURE[structure.lower()]
                         if structure is not None else structure
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
    def __init__(self, name, structure=None, structure_parameters=None,
                 source_parameters=None):

        super().__init__(name, structure, structure_parameters,
                         source_parameters)

        # dbname and sql are mandatory we take the others as options.
        self.metadata["dbname"] = source_parameters.pop("dbname")
        self.metadata["sql"] = source_parameters.pop("sql")

        self.metadata["db_kwargs"] = source_parameters

    def load(self):
        import psycopg2
        with psycopg2.connect(dbname=self.metadata["dbname"],
                              **self.metadata["db_kwargs"]) as con:
            cur = con.cursor()
            cur.execute(self.metadata["sql"])
            colnames = [desc[0] for desc in cur.description]
            self._payload = pd.DataFrame(cur.fetchall(), columns=colnames)

        super().load()


class SourceFlatFile(DataSource):
    def __init__(self, name, structure=None, structure_parameters=None,
                 source_parameters=None):

        super().__init__(name, structure, structure_parameters,
                         source_parameters)

        self.metadata["filename"] = source_parameters["filename"]
        self.metadata["delimiter"] = source_parameters.get("delimiter", ",")

    def load(self):
        self._payload = pd.read_csv(self.metadata["filename"],
                                    sep=self.metadata["delimiter"])
        super().load()
