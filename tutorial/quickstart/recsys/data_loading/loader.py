import pandas as pd
import numpy as np
import os
import time
from abc import ABCMeta, abstractmethod
from pymongo import MongoClient


def iterator2dataframes(iterator, chunk_size):
    """Turn an iterator into multiple small pandas.DataFrame
    This is a balance between memory and efficiency
    """
    records = []
    frames = []
    for i, record in enumerate(iterator):
        records.append(record)
        if i % chunk_size == chunk_size - 1:
            frames.append(pd.DataFrame(records))
            records = []
    if records:
        frames.append(pd.DataFrame(records))
    return pd.concat(frames)


class Loader:
    def __init__(self):
        self.fileName = None
        self.identityFields = []

        if not self.collectionName:
            self.collectionName = None

        self.dbName = 'recsys'
        self.indexField = None
        self.df = None
        self.df_raw = None

        mongo = MongoClient()
        self.db = mongo[self.dbName]
        self.collection = self.db[self.collectionName]

    def load(self):
        file_path = self.get_file_path_with_ext('pkl')
        df = pd.read_pickle(file_path)
        self.df = df
        return self

    def load_raw(self, force=False):
        if not force and self.df_raw is not None:
            return self

        self.df_raw = iterator2dataframes(self.collection.find({}), 200000)
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
        df_raw = self.df_raw.copy()
        df_raw = self.do_prepare_raw_data_save(df_raw)
        file_path = self.get_file_path_with_ext('pkl')
        df_raw.to_pickle(file_path)

    @abstractmethod
    def do_prepare_raw_data_save(self, df_raw):
        pass

    def get_identity_where(self, row):
        where = {}
        for field in self.identityFields:
            where[field] = row[field]

        return where

    def add_rows(self, rows):
        for row in rows:
            where = self.get_identity_where(row)
            self.collection.update_one(where, {'$set': row}, upsert=True)

        self.load_raw(force=True).prepare_raw_and_save()
