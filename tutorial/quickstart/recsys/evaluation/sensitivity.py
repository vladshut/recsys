class Sensitivity:
    def __init__(self, recommendation_list, test_coupons_ids=None):
        if test_coupons_ids is None:
            test_coupons_ids = []

        self.recommendationList = recommendation_list
        self.testCouponsIds = test_coupons_ids

        pass

    def add(self, coupon_id):
        self.testCouponsIds.append(coupon_id)
        pass

    def compute(self):
        positives = len(self.testCouponsIds)

        true_positives = .0
        for recommendCouponId in self.recommendationList:
            if recommendCouponId in self.testCouponsIds:
                true_positives += 1.0

        sensitivity = float(true_positives) / float(positives)

        return sensitivity

