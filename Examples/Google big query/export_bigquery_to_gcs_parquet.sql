--exporting data to gcs in parquet format
EXPORT DATA
  OPTIONS( uri=CONCAT('gs://bucket/folder/*.parquet'),
    format='PARQUET',
    OVERWRITE=TRUE ) AS SELECT * FROM `project.dataset.table` 