import pandas as pd
import os
import time
from ..similarity.my import My as Similarity
from ..similarity.dice import Dice as DiceSimilarity
from ..data_loading.users_loader import UsersLoader
from ..data_loading.ratings_loader import RatingsLoader
from itertools import islice
from collections import OrderedDict
from sklearn import preprocessing


def take(n, iterable):
    return list(islice(iterable, n))


class AlgorithmException(Exception):
    pass


class Algorithm:
    MOST_SIMILAR_USERS_NUMBER = 100

    def __init__(self, rating_data=None, user_data=None):
        if rating_data is None:
            ratings_loader = RatingsLoader()
            self.ratingData = ratings_loader.load().get_df()

        if user_data is None:
            users_loader = UsersLoader()
            self.userData = users_loader.load().get_df()

        self.activeUserCouponsRatingData = None
        self.similarity = Similarity
        self.usersRecommendations = {}

    def compute_similarity(self, u1, u2):
        return self.similarity.compute(u1, u2)

    def compute_purchase_similarity(self, coupons_bought_by_active_user, uid):
        coupons_bought_by_user = self.activeUserCouponsRatingData.loc[
            self.activeUserCouponsRatingData['patient_id'] == uid
        ]['coupon_id'].unique()

        au_purchase_vector = [True] * len(coupons_bought_by_active_user)
        u_purchase_vector = []

        for a_coupon_id in coupons_bought_by_active_user:
            u_purchase_vector.append(a_coupon_id in coupons_bought_by_user)

        return DiceSimilarity.compute(au_purchase_vector, u_purchase_vector)

    def vectorize_user_data(self, u):
        vector = [float(u[key]) for key in ['age', 'sex', 'city_population_rank']]
        return vector

    def get_user_recommendations(self, auid):
        if auid in self.usersRecommendations:
            return self.usersRecommendations[auid]

        if auid not in self.userData.index:
            raise AlgorithmException('User with id \'' + str(auid) + '\' not loaded!')

        similar_users = {}

        coupons_bought_by_active_user = self.ratingData.loc[self.ratingData['patient_id'] == auid]['coupon_id'].unique()
        coupons_number_bought_by_user = len(coupons_bought_by_active_user)

        print ('COUPONS NUMBER BOUGHT BY USER: ' + str(coupons_number_bought_by_user))

        a_user_data = self.userData.ix[auid].to_dict()
        a_u_vector = self.vectorize_user_data(a_user_data)
        a_user_data['patient_id'] = auid

        if coupons_number_bought_by_user == 0:
            users_ids_to_find_similar_users = self.ratingData['patient_id'].unique()
        else:
            a_u_vector.append(1.0)
            patient_ids = self.ratingData.loc[
                self.ratingData['coupon_id'].isin(coupons_bought_by_active_user)
            ]['patient_id'].unique()

            ratings_with_coupons = self.ratingData.loc[
                ~self.ratingData['coupon_id'].isin(coupons_bought_by_active_user) &
                self.ratingData['patient_id'].isin(patient_ids)
            ]
            users_ids_to_find_similar_users = ratings_with_coupons['patient_id'].unique()

            if len(users_ids_to_find_similar_users) == 0:
                users_ids_to_find_similar_users = self.ratingData['patient_id'].unique()

        df_similar_users = self.userData.loc[
            self.userData.index.isin(users_ids_to_find_similar_users) &
            (self.userData['sex'] == a_user_data['sex'])
        ].head(1000)

        users_ids_to_find_similar_users = df_similar_users.index.tolist()

        rating_data_cutted = self.ratingData[self.ratingData['patient_id'].isin(users_ids_to_find_similar_users)]
        self.activeUserCouponsRatingData = rating_data_cutted.loc[rating_data_cutted['coupon_id'].isin(coupons_bought_by_active_user)]

        print ('SIMILAR USERS COUNT: ' + str(len(users_ids_to_find_similar_users)))

        for index, row in df_similar_users.iterrows():
            row['patient_id'] = index
            if row['patient_id'] == auid:
                continue

            user_data = row.to_dict()
            u_vector = self.vectorize_user_data(user_data)
            user_data['patient_id'] = index

            if coupons_number_bought_by_user > 0:
                purchase_similarity = self.compute_purchase_similarity(coupons_bought_by_active_user, index)
                u_vector.append(purchase_similarity)

            similarity = self.compute_similarity(a_u_vector, u_vector)
            similar_users[index] = similarity

        similar_users = OrderedDict(sorted(similar_users.items(), key=lambda kv: kv[1], reverse=True))
        most_similar_users = take(self.MOST_SIMILAR_USERS_NUMBER, similar_users.iteritems())

        similar_users_ratings_list = []

        for similarUser in most_similar_users:
            similar_user_ratings = rating_data_cutted.loc[rating_data_cutted['patient_id'] == similarUser[0], :]
            similar_user_ratings.loc[:, 'rating'] = similar_user_ratings.loc[:, 'rating'].apply(
                lambda x: float(x) * similarUser[1])
            similar_users_ratings_list.append(similar_user_ratings)

        if len(similar_users_ratings_list) == 0:
            print 'empty'
            self.usersRecommendations[auid] = False
            return self.usersRecommendations[auid]

        similar_users_ratings = pd.concat(similar_users_ratings_list, ignore_index=True).groupby('coupon_id', as_index=False)['rating'].sum()

        similar_users_ratings['rating'] = similar_users_ratings['rating'].apply(lambda x: 1.0 * x / len(most_similar_users))
        similar_users_ratings = similar_users_ratings.sort(columns='rating', ascending=False)
        similar_users_ratings = similar_users_ratings[~similar_users_ratings['coupon_id'].isin(coupons_bought_by_active_user)]

        self.usersRecommendations[auid] = similar_users_ratings

        return self.usersRecommendations[auid]

    def get_top_user_recommendations(self, auid, n):
        recommendations = self.get_user_recommendations(auid)

        if len(recommendations) == 0:
            return []

        recommendations = recommendations.groupby(['coupon_id']).sum().sort(['rating'], ascending=False).head(n)
        recommendations = recommendations['rating'].index.tolist()
        recommendations = [int(i) for i in recommendations]

        return recommendations

