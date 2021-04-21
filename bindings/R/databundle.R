# This provides basic deserialization for bundles created by databundle.

deserialize.parquet <- function(databundle_filename, data_source_name) {

  if (!endsWith(data_source_name, ".parquet")) {
    data_source_name <- paste0(data_source_name, ".parquet")
  }

  # Untar relevant member.
  dir.create("rdatabundle_temp")
  untar(databundle_filename, files=data_source_name, exdir="rdatabundle_temp")

  data <- arrow::read_parquet(paste0("rdatabundle_temp/", data_source_name))

  # Cleanup
  unlink("rdatabundle_temp", recursive=T)

  return(data)

}
