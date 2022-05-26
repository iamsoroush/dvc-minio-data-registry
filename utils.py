from pathlib import Path
import shutil
import typing

import SimpleITK as sitk
import pydicom
from pydicom import dcmread


def get_qualified_series_id_for_study(study_path: Path,
                                      min_dcm_files: int = 10,
                                      min_slice_thickness: float = 4) -> typing.List[str]:
    dcm_tags_to_check: dict = {'Modality': 'CT',
                               'BodyPartExamined': 'HEAD'}
    image_type_ = "AXIAL"

    qualified = list()
    if study_path.is_dir():
        series_ids = sitk.ImageSeriesReader.GetGDCMSeriesIDs(str(study_path))
        for sid in series_ids:
            series_file_names = sitk.ImageSeriesReader.GetGDCMSeriesFileNames(str(study_path), sid)
            dcm_file = dcmread(series_file_names[0], stop_before_pixels=True)

            conditions = list()
            for tag in dcm_tags_to_check:
                conditions.append(eval('dcm_file.' + tag + '.upper()') == dcm_tags_to_check[tag].upper())
            conditions.append(image_type_.upper() in [i.upper() for i in dcm_file.ImageType])
            conditions.append(len(series_file_names) >= min_dcm_files)
            conditions.append(float(dcm_file.SliceThickness) >= min_slice_thickness)

            if all(conditions):
                qualified.append(sid)

    return qualified


def anonymize(dataset: pydicom.Dataset):
    def person_names_callback(dataset, data_element):
        if data_element.VR == "PN":
            data_element.value = "anonymous"

    def curves_callback(dataset, data_element):
        if data_element.tag.group & 0xFF00 == 0x5000:
            del dataset[data_element.tag]

    dataset.PatientID = "id"
    dataset.walk(person_names_callback)
    dataset.walk(curves_callback)


def delete_folder_content(folder: Path):
    for file_path in folder.iterdir():
        try:
            if file_path.is_file() or file_path.is_symlink():
                file_path.unlink()
            elif file_path.is_dir():
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))


def get_dicom_tags() -> list:
    dicom_tags = ['StudyInstanceUID',
                  'SeriesInstanceUID',
                  'Modality',
                  'BodyPartExamined',
                  'StudyDescription',
                  'SeriesDescription',
                  'Manufacturer',
                  'ManufacturerModelName',
                  'SpatialResolution',
                  'PatientAge',
                  'PatientSex',
                  'ImagesInAcquisition',
                  'LossyImageCompression',
                  'SliceThickness',
                  'PixelSpacing',
                  'SamplesPerPixel']
    return dicom_tags


def get_series_info(series_path: Path) -> dict:
    dicom_tags = {k: None for k in get_dicom_tags()}

    float_tags = ['SpatialResolution', 'SliceThickness']

    series_id = sitk.ImageSeriesReader.GetGDCMSeriesIDs(str(series_path))[0]
    series_file_names = sitk.ImageSeriesReader.GetGDCMSeriesFileNames(str(series_path), series_id)

    with dcmread(series_file_names[0], stop_before_pixels=True) as dicom_file:
        for dcm_tag in dicom_tags.keys():
            try:
                val = getattr(dicom_file, dcm_tag)
                if dcm_tag in float_tags:
                    dicom_tags[dcm_tag] = float(val)
                elif dcm_tag == 'SamplesPerPixel':
                    dicom_tags[dcm_tag] = int(val)
                elif dcm_tag == 'PatientAge':
                    if isinstance(val, str):
                        val_lower = val.lower()
                        if '/' not in val_lower:
                            dicom_tags[dcm_tag] = int(val_lower[:val_lower.index('y')])
                        else:
                            dicom_tags[dcm_tag] = int(val_lower[:val_lower.index('/')])
                    else:
                        dicom_tags[dcm_tag] = val

                else:
                    dicom_tags[dcm_tag] = val

            except AttributeError:
                pass

    dicom_tags['NumberOfSlices'] = len(series_file_names)
    return dicom_tags
