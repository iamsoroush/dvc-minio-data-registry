from pathlib import Path
import shutil

import pandas as pd
from pandas_profiling import ProfileReport
from metaflow import FlowSpec, step, Parameter
import SimpleITK as sitk
import pydicom

from utils import get_qualified_series_id_for_study, anonymize, delete_folder_content, get_series_info


class DataRegistrationPipeline(FlowSpec):
    src_dir: str = Parameter('src-dir',
                             required=True)
    data_source_name: str = Parameter('datasource-name',
                                      required=True)
    overwrite: bool = Parameter('overwrite',
                                default=False)
    datasets_folder: str = "datasources"

    ct_window_l: int = 40  # center of the window, for generating histogram values
    ct_window_w: int = 300  # width of the window

    @step
    def start(self):
        print("data source path: ", self.src_dir)
        self.next(self.extract_study_paths)

    @step
    def extract_study_paths(self):
        def walk_into_dir(p: Path):
            paths = list(p.iterdir())
            is_file = [i.is_file() for i in paths]
            is_dcm_file = [i for i in paths if i.name.endswith('.dcm')]
            is_dir = [i.is_dir() for i in paths]
            is_empty = not any(paths)

            if is_empty:
                print(f'{p}: an empty directory')
                return None
            elif all(is_file) and any(is_dcm_file):
                return [p]
            elif any(is_dir):
                study_paths = list()
                for i in paths:
                    if i.is_dir():
                        res = walk_into_dir(i)
                        if res is not None:
                            study_paths.extend(res)
                return study_paths
            else:
                print(f'{p}: else!')
                return None

        self.ds_dir = Path(self.src_dir)
        self.study_paths = walk_into_dir(self.ds_dir)
        print(f'extracted {len(self.study_paths)} studies.')
        self.next(self.extract_qualified_series)

    @step
    def extract_qualified_series(self):
        self.qualified_series = {}

        for study_path in self.study_paths:
            series_ids = get_qualified_series_id_for_study(study_path)
            print(f'{len(series_ids)} qualified series for {study_path.name}')
            if any(series_ids):
                self.qualified_series[study_path] = series_ids[0]

        print(f'extracted {len(self.qualified_series)} qualified series')

        self.next(self.copy_to_dst)

    @step
    def copy_to_dst(self):
        dst = Path(self.datasets_folder).joinpath(self.data_source_name)
        dst.mkdir(exist_ok=True, parents=True)
        self.dst = dst

        self.dst_series_paths = list()

        for study_path, sid in self.qualified_series.items():
            # for sid in sids:
            series_dst = dst.joinpath(sid)
            if series_dst.exists():
                if not self.overwrite:
                    print(f'series {sid} is already transformed, skipping ..')
                    continue
                else:
                    print(f'WARNING: series {series_dst} exist, overwriting ..')
                    delete_folder_content(series_dst)

            series_dst.mkdir()
            self.dst_series_paths.append(series_dst)

            series_file_names = sitk.ImageSeriesReader.GetGDCMSeriesFileNames(str(study_path), sid)

            print(f'transforming files to {series_dst}')
            for sfn in series_file_names:
                shutil.copyfile(sfn, series_dst.joinpath(Path(sfn).name))
        self.next(self.anonymize)

    @step
    def anonymize(self):
        for series_path in self.dst_series_paths:
            if series_path.is_dir():
                print(f'anonymizing series {series_path.name}')
                for dcm_file in series_path.iterdir():
                    with pydicom.dcmread(dcm_file) as dataset:
                        anonymize(dataset)
                    dataset.save_as(dcm_file, write_like_original=True)

        self.next(self.create_meta_data_file)

    @step
    def create_meta_data_file(self):
        infos = list()

        for series_path in self.dst.iterdir():
            if series_path.is_dir():
                infos.append(get_series_info(series_path))

        self.meta_data = pd.DataFrame(infos)
        self.next(self.add_hu_histogram)

    @step
    def add_hu_histogram(self):

        self.next(self.summarize_and_write_meta_data)

    @step
    def summarize_and_write_meta_data(self):
        self.meta_data.to_csv(self.dst.joinpath('meta-data.csv'), index=False)
        profile = ProfileReport(self.meta_data, title=f"{self.data_source_name} DataSource's Profiling Report")
        profile.to_file(self.dst.joinpath('meta-data-report.html'))

        self.next(self.push_to_remote)

    @step
    def push_to_remote(self):
        # cmd = f'DATA_ROOT="{self.datasets_folder}" TARGET_DS_NAME="{self.data_source_name}" ADDED_DS_PATH="{self.src_dir}" sh dvcpush.sh '
        # stream = os.popen(cmd)
        # print(stream.read().strip())

        self.next(self.end)

    @step
    def end(self):
        print('Done.')
        pass


if __name__ == '__main__':
    DataRegistrationPipeline()


