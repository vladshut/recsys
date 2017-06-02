import pandas as pd
import numpy as np
import os
from abc import ABCMeta, abstractmethod
from pymongo import MongoClient


class Loader:
    def __init__(self):
        self.fileName = None
        self.indexField = None
        self.df = None
        self.df_raw = None

    def load(self):
        file_path = self.get_file_path_with_ext('pkl')
        df = pd.read_pickle(file_path)
        self.df = df
        return self

    def load_raw(self, force=False):
        if not force and self.df_raw is not None:
            return self

        file_path = self.get_file_path_with_ext('csv')

        df = pd.read_csv(file_path)

        self.df_raw = df
        return self

    def get_df(self):
        return self.df

    def get_df_raw(self):
        return self.df_raw

    def get_file_path_with_ext(self, ext):
        current_dir = os.path.dirname(__file__)
        path = os.path.join(current_dir, '../data/vita/' + self.fileName + '.' + ext)
        return path

    def prepare_raw_and_save(self):
        df_raw = self.df_raw
        df_raw = self.do_prepare_raw_data_save(df_raw)
        file_path = self.get_file_path_with_ext('pkl')
        # df_raw.to_pickle(file_path)
        raise KeyError(1)
        df_raw.to_csv(file_path, index=False)

        # return self.load_raw(force=True)

    @abstractmethod
    def do_prepare_raw_data_save(self, df_raw):
        pass

    def add_row(self, row):
        df_raw = self.load_raw().get_df_raw()
        df_raw = df_raw[['sex', 'patient_id', 'age', 'city_population_rank']]
        file_path = self.get_file_path_with_ext('csv')

        df_raw.to_csv(file_path, index=False)
        raise KeyError(1)

        # df_raw = df_raw.drop(df_raw[df_raw[self.indexField] ==
        #                             row[self.indexField]].index)
        # row_s = {k:pd.Series([str(v)], index=[0]) for k,v in row.iteritems()}
        # row_df = pd.DataFrame.from_dict(row_s)
        # df_raw = df_raw.append(row_df, ignore_index=True)
        # file_path = self.get_file_path_with_ext('csv')
        # df_raw.to_csv(file_path, index=False)
        self.load_raw(force=True).prepare_raw_and_save()
