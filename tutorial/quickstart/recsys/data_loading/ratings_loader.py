from loader import Loader


class RatingsLoader(Loader):
    def __init__(self):
        self.collectionName = 'ratings'
        Loader.__init__(self)
        self.identityFields = ['patient_id', 'coupon_id']
        self.fileName = 'coupon_patient_2'

    def do_prepare_raw_data_save(self, df_raw):
        df_raw = df_raw.sample(frac=1)

        return df_raw