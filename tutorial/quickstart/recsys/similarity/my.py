import scipy.spatial.distance as dst


class My:
    def __init__(self):
        pass

    @staticmethod
    def compute(u1, u2):

        return dst.euclidean(u1, u2)
        sex_score = 0
        if u1['sex'] == u2['sex']:
            sex_score = 1

        age_score = (1 - (abs(u1['age'] - u2['age']) / 60))
        if age_score < 0:
            age_score = 0

        if not u1['city_population_rank'] or not u2['city_population_rank']:
            city_score = 0
        else:
            city_score = (1 - (abs(u1['city_population_rank'] - u2['city_population_rank']) / 5))
            if city_score < 0:
                city_score = 0

        score = (sex_score + age_score + city_score) / 3

        return score
