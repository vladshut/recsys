# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from recsys.algorithm.algorithm import Algorithm as RecSys, AlgorithmException as RecSysException
from django.contrib.auth.models import User, Group
from rest_framework import viewsets
from tutorial.quickstart.serializers import UserSerializer, GroupSerializer
from django.http import JsonResponse
import pandas as pd

import time
import json

from recsys.data_loading.ratings_loader import RatingsLoader
from recsys.data_loading.users_loader import UsersLoader
from recsys.data_loading.cities_loader import get_population_rank_from_population_number
from recsys.algorithm.algorithm import Algorithm, AlgorithmException
from recsys.evaluation.sensitivity import Sensitivity as EvaluationMetric

from django.views.decorators.csrf import csrf_exempt


@csrf_exempt
def add(request):

    print 'Start'
    response = {}
    users_json = request.POST.get('users', None)

    if users_json is None:
        response['status'] = 'error'
        response['message'] = 'Invalid input data. Data is missed.'
        return JsonResponse(response)

    users = json.loads(users_json)

    users_loader = UsersLoader()
    ratings_loader = RatingsLoader()

    for user in users:
        # TODO: validate input data (
        #   age is numeric [0, 100],
        #   population is numeric [1000, 2000000],
        #   sex is numeric [0,1], id matches ID pattern
        # )

        # prepare data (scale popularity)
        prepared_user = {
            '_id': user['id'],
            'age': float(user['age']),
            'sex': user['sex'],
            'city_population_rank': get_population_rank_from_population_number(user['city_population'])
        }

        # load raw user data from csv to dataframe
        # add (or update) user to dataframe and save dataframe to csv
        # update pkl file from csv (preprocessing data)
        users_loader.add_rows([prepared_user])
        print 'user was added'
        # load raw rating data from csv to dataframe
        # add (if does not exist) user rating data to dataframe and save dataframe to csv
        # update pkl file from csv (preprocessing data)
        for rating in user['ratings']:
            prepared_rating = {
                'coupon_id': rating['coupon_id'],
                'rating': 1,
                'patient_id': user['id']
            }

            ratings_loader.add_rows([prepared_rating])

    response['status'] = 'ok'
    return JsonResponse(response)


def check(request):
    # load pickle
    # check if user exists
    return JsonResponse(True)


def recommendations(request):
    user_ids = request.GET.getlist('user_id', [])

    if len(user_ids) == 0:
	print 'user_ids is empty'
        JsonResponse({})

    rec_sys = RecSys()

    result = {}

    for user_id in user_ids:
	print str(user_id)
        try:
            recommendations = rec_sys.get_top_user_recommendations(user_id, 15)
        except RecSysException as e:
            print (str(e))
            recommendations = []

        result[user_id] = recommendations

    return JsonResponse(result)


def testing(request):
    '''''''''''''''''''''''''''''''''''''''
        PARAMS
    '''''''''''''''''''''''''''''''''''''''
    NUMBER_OF_ITERATIONS = 10
    TRAIN_TEST_PERCENT = 80
    TEST_CASES_NUMBER = 50
    MIN_TOP_COUPONS_NUMBER = 1
    MAX_TOP_COUPONS_NUMBER = 20

    # Datasets
    usersLoader = UsersLoader('recsys/data/vita/patients_new')
    dfUsers = usersLoader.load().get_df()

    ratingsLoader = RatingsLoader('recsys/data/vita/coupon_patient_2')
    dfRatings = ratingsLoader.load().get_df()

    sensitivities = {}

    for iteration in range(1, NUMBER_OF_ITERATIONS + 1):
        usersCouponPredictions = {}
        dfRatingTrain, dfRatingTest = ratingsLoader.split_df(TRAIN_TEST_PERCENT)
        # print(dfRatings.count, dfRatingTrain.count, dfRatingTest.count)
        dfRatingTest = dfRatingTest.sample(TEST_CASES_NUMBER)
        algorithm = Algorithm(dfRatingTrain, dfUsers)
        start_time = time.time()
        testUsersIds = dfRatingTest.loc[:, 'patient_id'].unique()
        print 'Number of test users: ' + str(len(testUsersIds))

        aUserNumber = 1
        for aUserId in testUsersIds:
            try:
                result = algorithm.get_user_recommendations(aUserId)

                if isinstance(result, pd.DataFrame):
                    usersCouponPredictions[aUserId] = result

            except AlgorithmException as e:
                continue

            aUserNumber += 1

            if aUserNumber % 10 == 0:
                print 'active users precessed: ' + str(aUserNumber)

        if len(usersCouponPredictions) == 0:
            print '======================'
            print 'empty usersCouponPredictions'
            print 'continue ..............'

        '''''
            CALCULATE ERRORS
        '''''

        for topCouponsNumber in range(MIN_TOP_COUPONS_NUMBER, MAX_TOP_COUPONS_NUMBER + 1):
            sensitivitySum = .0
            for userId in usersCouponPredictions:
                usersCouponPredictionsTop = usersCouponPredictions[userId].head(topCouponsNumber)
                userCoupons = dfRatingTest.loc[dfRatingTest['patient_id'] == userId]

                usersCouponsPrediction = usersCouponPredictionsTop['coupon_id'].tolist()
                usersCouponsTest = userCoupons['coupon_id'].tolist()
                evaluation = EvaluationMetric(usersCouponsPrediction, usersCouponsTest)
                evaluationValue = evaluation.compute()

                sensitivitySum += evaluationValue

            if len(usersCouponPredictions) == 0:
                sensitivity = 0
            else:
                sensitivity = sensitivitySum / len(usersCouponPredictions)

            if topCouponsNumber not in sensitivities:
                sensitivities[topCouponsNumber] = []
            sensitivities[topCouponsNumber].append(sensitivity)

        print '==============================================================='
        print 'iteration = '
        print iteration
        print 'sensitivity = '
        print sensitivities
        print("--- %s seconds ---" % (time.time() - start_time))
        print '==============================================================='

    for topNumber in sensitivities:
        print '==================================================================='
        print 'topCouponsNumber = '
        print topNumber
        print 'AVERAGE SENSITIVITY = '
        print float(sum(sensitivities[topNumber])) / float(len(sensitivities[topNumber]))

    '''''
        K = max number of users found by rating similarity
        KVal = min similarity value
        if userId bought <= 1 coupon
            find similar users who bought same coupon and has same demographic data
            find similar users
            coupons = get recommendations from similar users
        else if userId bought coupons > 1
            find similar users by purchases
            find similar users by demographic data
            compute general similarity
            find similar users
            coupons = get recommendations from similar users


        if coupons.length < required number
            push coupons from TOP N

    '''''

    return JsonResponse(12223)


class Add(viewsets.ModelViewSet):
    queryset = Group.objects.all()
    serializer_class = GroupSerializer


class Check(viewsets.ModelViewSet):
    queryset = Group.objects.all()
    serializer_class = GroupSerializer


class Recommendations(viewsets.ModelViewSet):
    queryset = Group.objects.all()
    serializer_class = GroupSerializer


class Testing(viewsets.ModelViewSet):
    queryset = Group.objects.all()
    serializer_class = GroupSerializer


class UserViewSet(viewsets.ModelViewSet):
    """
    API endpoint that allows users to be viewed or edited.
    """
    queryset = User.objects.all().order_by('-date_joined')
    serializer_class = UserSerializer


class GroupViewSet(viewsets.ModelViewSet):
    """
    API endpoint that allows groups to be viewed or edited.
    """
    queryset = Group.objects.all()
    serializer_class = GroupSerializer
