# dvc-minio-data-registry
A sample repository for using as a data registry which handles versioning the data using DVC and uses a minIO S3 object-storage.

# Set-up
1. `pip install -r requirements.txt`
2. `dvc init`
3. `dvc remote add -d minio s3://ctbrain-data -f`
4. `dvc remote modify minio endpointurl http://156.253.5.38:9000`
5. `dvc remote modify minio access_key_id my_login`
6. `dvc remote modify minio secret_access_key my_password`

# Add a data-source
1. define your data-source name and the path to the data, which is a folder containing Dicom Studies, which are collections of dicom files.
2. `python add_to_data_source.py run --src-dir "/Users/soroush/Datasets/CT Brain/local/Artifact" --datasource-name pacs`
3. `dvc add "${DATA_ROOT}/${TARGET_DS_NAME}"`
4. `dvc push`
5. `git add "${DATA_ROOT}/*.dvc" "${DATA_ROOT}/.gitignore" .gitignore .dvcignore .dvc/config .dvc/.gitignore`
6. `git commit .dvc/.gitignore .dvc/co
nfig .dvcignore datasources/.gitignore datasources/pacs.dvc -m "add local_data/Artifact to pacs dataset in datasources"`
7. `git tag ctbrain-init`
8. `git push origin ctbrain-init`

# Create/Update a task meta-data.csv
you can have slice-level or series-level labels.

**Note**: slice indices will be indicated by reading the pixel_data via `SimpleITK`:
```Python
from pathlib import Path
import SimpleITK as sitk

study_path = Path('...')
series_ids = sitk.ImageSeriesReader.GetGDCMSeriesIDs(str(study_path))
series_file_names = sitk.ImageSeriesReader.GetGDCMSeriesFileNames(str(study_path), series_ids[0])  # This is an AXIAL CT series
reader = sitk.ImageSeriesReader()
reader.SetFileNames(series_file_names)
image = reader.Execute()
image.GetSize()  # (512, 512, n_slices)
```

**Note**: You can use the `split` parameter:
- `train` -> add the data points as training data
- `eval` -> add the data points as evaluation data
- `None`, `not specified` -> add the data points and split them randomly (`stratified`)


## Slice-Level labels
1. prepare a csv file in which each row is a data-point, i.e. a slice
2. each row has to have at least four attributes:
   1. `DataSource` -> the datasource name under the `datasources` folder, for instance `'pacs'`
   2. `SeriesInstacnceUID`
   3. `SliceIndex`
   4. `Label`
   5. you can add more columns, e.g. the labeler X's opinion.
3. trigger the pipeline `add_meta_data` by passing the task name and other parameters

