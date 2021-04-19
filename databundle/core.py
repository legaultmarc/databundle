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


def datasources_from_yaml_stream(stream):
    conf = yaml.safe_load(stream)

    data_loaders = {
        "flat_file": SourceFlatFile
    }

    for source in conf["sources"]:
        # Get the right data loader.
        dl = data_loaders[source.pop("type")](**source)
        dl.load()
        print(dl._payload)


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


class DataSource(object):
    def __init__(self, name, structure, structure_parameters=None,
                 source_parameters=None):

        self.metadata = {
            "structure": STR_TO_STRUCTURE[structure.lower()]
        }
        self.name = name


        self.structure_parameters = {}
        if structure_parameters is not None:
            self.structure_parameters = structure_parameters

        self.source_parameters = {}
        if source_parameters:
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

        # Compute the missigness matrix.
        self._missing = self._encode_missing()

    def _encode_missing(self, sparse=True):
        # For dense matrices, we can either store NaNs as a dense binary matrix
        # or in the "long/tidy" format.
        # This is controlled by the sparse parameter.
        df = self._payload
        if self.metadata["structure"] == "dense":
            if sparse:
                t = df.melt(id_vars="sample_id")
                return t.loc[t["value"].isnull(), ["sample_id", "variable"]]
            else:
                return df.isna().values

        elif self.metadata["structure"] == "sparse":
            # Return an numpy array of missing samples.
            return df.loc[df.iloc[:, 1].isna(), :].iloc[:, 0].values

        elif self.metadata["structure"] == "long":
            # Return a data frame of (sample_id, variable_id).
            return df.loc[df.iloc[:, 2].isna(), :].iloc[:, [0, 1]]

    def serialize(self):



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
