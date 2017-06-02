from sklearn import preprocessing
from loader import Loader


class UsersLoader(Loader):
    def __init__(self):
        self.collectionName = 'users'
        self.identityFields = ['_id']
        Loader.__init__(self)
        self.fileName = 'patients'
        self.indexField = 'patient_id'

    def do_prepare_raw_data_save(self, df_raw):

        df_raw.set_index('_id', inplace=True)
        df_raw = df_raw[df_raw['age'] <= 100]
        df_raw = df_raw[df_raw['city_population_rank'] > 0]
        df_raw['sex'] = df_raw['sex'].map({'f': 1.0, 'm': 0.0})
        df_raw['city_population_rank'] = preprocessing.scale(df_raw['city_population_rank'])
        df_raw['age'] = preprocessing.scale(df_raw['age'])
        df_raw = df_raw.sample(frac=1)
        df_raw = df_raw[['sex', 'age', 'city_population_rank']]

        return df_raw

    def get_identity_where(self, row):
        return {'_id': row['_id']}