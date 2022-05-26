from pathlib import Path
import shutil

import pandas as pd
from pandas_profiling import ProfileReport
from metaflow import FlowSpec, step, Parameter
import SimpleITK as sitk
import pydicom

from utils import get_qualified_series_id_for_study, anonymize, delete_folder_content, get_series_info


class LinearFlow(FlowSpec):
    src_dir: str = Parameter('src-dir',
                             required=True)
    data_source_name: str = Parameter('datasource-name',
                                      required=True)
    overwrite: bool = Parameter('overwrite',
                                default=False)
    datasets_folder: str = "datasources"


    @step
    def start(self):
        print("data source path: ", self.src_dir)
        self.next(self.extract_qualified_series)


    @step
    def extract_qualified_series(self):
        p = Path(self.src_dir)
        # print(f'number of series: {len(list(p.iterdir()))}')
        self.qualified_series = {}

        for study_path in p.iterdir():
            series_ids = get_qualified_series_id_for_study(study_path)
            if any(series_ids):
                self.qualified_series[study_path] = series_ids[0]

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
                # anonymizer = dicognito.anonymizer.Anonymizer(id_prefix="AIMedic")
                for dcm_file in series_path.iterdir():
                    with pydicom.dcmread(dcm_file) as dataset:
                        # anonymizer.anonymize(dataset)
                        anonymize(dataset)
                    dataset.save_as(dcm_file, write_like_original=True)

        self.next(self.create_meta_data_file)

    @step
    def create_meta_data_file(self):
        infos = list()

        for series_path in self.dst.iterdir():
            if series_path.is_dir():
                infos.append(get_series_info(series_path))

        df = pd.DataFrame(infos)
        df.to_csv(self.dst.joinpath('meta-data.csv'), index=False)
        profile = ProfileReport(df, title=f"{self.data_source_name} DataSource's Profiling Report")
        profile.to_file(self.dst.joinpath('meta-data-report.html'))

        self.next(self.push_to_remote)

    @step
    def push_to_remote(self):
        # commands = list()
        #
        # commands.append(f'git add "{self.datasets_folder}/*.dvc" "{self.datasets_folder}/.gitignore" .gitignore .dvcignore .dvc/config .dvc/.gitignore')
        # commands.append(f'git commit -m "track the {self.data_source_name} dataset in {self.datasets_folder}"')
        # commands.append(f'dvc push')
        #
        # for cmd in commands:
        #     stream = os.popen(cmd)
        #     print(stream.read().strip())

        # cmd = f'DATA_ROOT="{self.datasets_folder}" TARGET_DS_NAME="{self.data_source_name}" ADDED_DS_PATH="{self.src_dir}" sh dvcpush.sh '
        # stream = os.popen(cmd)
        # print(stream.read().strip())

        self.next(self.end)

    @step
    def end(self):
        # print(f'qualified series: ')
        # print(self.qualified_series)
        # print('the data source path is still: %s' % self.data_src_path)
        pass


if __name__ == '__main__':
    LinearFlow()


