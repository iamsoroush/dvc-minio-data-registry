from pathlib import Path

import numpy as np
import pandas as pd
from pandas_profiling import ProfileReport
from metaflow import FlowSpec, step, Parameter
from sklearn.model_selection import StratifiedShuffleSplit

from utils import get_dicom_tags

FILE_PATH = Path(__file__).parent.resolve()


class AddLabelsPipeline(FlowSpec):
    csv_file_path: str = Parameter('csv-file',
                                   required=True)
    task_name: str = Parameter('task-name',
                               required=True)
    split: str = Parameter('split',
                            default=None)
    eval_p: float = Parameter('eval-p',
                              default=0.1)

    datasets_folder: Path = FILE_PATH.joinpath('datasources')
    seed: int = 0

    @step
    def start(self):
        """Start the pipeline"""

        self.csv_file = pd.read_csv(self.csv_file_path)
        print("loaded csv file: ", self.csv_file_path)

        cols = self.csv_file.columns
        assert 'DataSource' in cols
        # assert 'StudyInstanceUID' in cols
        assert 'SeriesInstanceUID' in cols
        assert 'Label' in cols

        assert self.split in ('train', 'eval', None), 'split must be in (train, eval, None)'
        self.next(self.verify_meta_data)

    @step
    def verify_meta_data(self):
        assert set(self.csv_file['DataSource'].unique()).issubset([i.name for i in self.datasets_folder.iterdir() if i.is_dir()]), 'some datasource are not found in the datasources folder'

        for ind, (dsource, suid) in self.csv_file[['DataSource', 'SeriesInstanceUID']].iterrows():
            assert self.datasets_folder.joinpath(dsource).joinpath(suid).is_dir(), f'series does not exist for row number {ind}'

        self.next(self.create_meta_data_file)

    @step
    def create_meta_data_file(self):
        cols_to_add = get_dicom_tags()
        for col in self.csv_file.columns:
            try:
                cols_to_add.remove(col)
            except Exception:
                pass
            # else:
            #     self.csv_file[col] = None

        datasources_df = pd.concat([pd.read_csv(self.datasets_folder.joinpath(ds).joinpath('meta-data.csv'),
                                                index_col='SeriesInstanceUID') for ds in self.csv_file['DataSource'].unique()])

        self.task_path = FILE_PATH.joinpath(self.task_name)
        self.task_path.mkdir(exist_ok=True)
        self.task_meta_data_path = self.task_path.joinpath('meta-data.csv')

        if self.task_meta_data_path.is_file():
            task_meta_data = pd.read_csv(self.task_meta_data_path)
            print(f'{self.task_name}-meta-data exists, loaded.')
        else:
            task_meta_data = None

        unique_sids_in_labels_file = self.csv_file['SeriesInstanceUID'].unique()

        for sid in unique_sids_in_labels_file:
            if task_meta_data is not None:
                if sid in task_meta_data['SeriesInstanceUID'].values.tolist():
                    print(f'updating the meta-data for {sid}')
                    task_meta_data.drop(index=task_meta_data[task_meta_data['SeriesInstanceUID'] == sid].index,
                                        inplace=True)
                else:
                    print(f'adding the meta-data for {sid}')

                for col in cols_to_add:
                    print(f'adding {col} for {sid}')
                    self.csv_file.loc[self.csv_file['SeriesInstanceUID'] == sid, col] = datasources_df.loc[sid][col]
                if self.split in ('train', 'eval'):
                    self.csv_file.loc[self.csv_file['SeriesInstanceUID'] == sid, 'Split'] = self.split

        if task_meta_data is not None:
            self.new_meta_data = pd.concat([task_meta_data, self.csv_file])
        else:
            self.new_meta_data = self.csv_file

        if self.split not in ('train', 'eval'):
            print(f'splitting with eval = {self.eval_p}')
            self.split_stratified()

        self.next(self.generate_report)

    def split_stratified(self):
        unique_sids = self.new_meta_data['SeriesInstanceUID'].unique()
        labels = [self.new_meta_data[self.new_meta_data['SeriesInstanceUID'] == i]['Label'].values[0] for i in unique_sids.tolist()]

        sss = StratifiedShuffleSplit(n_splits=1, test_size=self.eval_p, random_state=self.seed)
        for train_ind, eval_ind in sss.split([None for _ in range(len(labels))], labels):
            train_sids = np.array(unique_sids)[train_ind]
            train_indices = self.new_meta_data[self.new_meta_data['SeriesInstanceUID'].isin(train_sids)].index

            eval_sids = np.array(unique_sids)[eval_ind]
            eval_indices = self.new_meta_data[self.new_meta_data['SeriesInstanceUID'].isin(eval_sids)].index

            print(f'{len(train_indices)} data-points for train, {len(eval_indices)} data-points for evaluation')

            self.new_meta_data.loc[train_indices, 'Split'] = 'train'
            self.new_meta_data.loc[eval_indices, 'Split'] = 'eval'

    @step
    def generate_report(self):
        self.new_meta_data.to_csv(self.task_meta_data_path, index=False)
        profile = ProfileReport(self.new_meta_data, title=f"{self.task_name}'s `meta-data` Profiling Report")
        profile.to_file(self.task_path.joinpath('meta-data-report.html'))

        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == '__main__':
    AddLabelsPipeline()
