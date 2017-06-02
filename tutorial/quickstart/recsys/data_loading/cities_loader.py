import pandas as pd
import numpy as np


def get_population_rank_from_population_number(population):
    population = int(population)
    if population < 50000:
        return 1

    if population < 100000:
        return 2

    if population < 340000:
        return 3

    if population < 650000:
        return 4

    return 5


class CitiesLoader:
    def __init__(self, file_path):
        self.filePath = file_path
        self.df = None

    def load(self):
        df = pd.read_csv(self.filePath, index_col='id', dtype={'id': int})
        df = df[np.isfinite(df['rank'])]
        self.df = df
        return self

    def get_df(self):
        return self.df

    def get_user_city_population_rank(self, u):
        try:
            city = self.df.ix[int(u['city_id'])].to_dict()
        except KeyError:
            return False

        if len(city) == 0:
            return False

        return city['rank']

    def city_population_rank(self, city_id):
        if city_id == np.nan or city_id == '':
            city_id = 0

        try:
            city = self.df.ix[int(city_id)].to_dict()
        except KeyError:
            return 0

        if len(city) == 0:
            return 0

        return int(city['rank'])

