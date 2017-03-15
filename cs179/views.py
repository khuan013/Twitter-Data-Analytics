from django.shortcuts import render
from django.http import HttpResponse

from cassandra.cluster import Cluster
cluster = Cluster(port=9043)
session = cluster.connect()

# Create your views here.
def index(request):
    return render(request, 'cs179/index.html')

def results(request):
    # form the query
    city = request.GET['city']
    state = request.GET['state']
    place = city + ", " + state

    gradeRow = session.execute("SELECT * FROM twitter.avggradebycity WHERE place = '%s'" % place)
    incomeRow = session.execute("SELECT * FROM twitter.avgincomebycity WHERE place = '%s'" % place)
    followersRow = session.execute("SELECT * FROM twitter.avgfollowersbycity WHERE place = '%s'" % place)
    statusesRow = session.execute("SELECT * FROM twitter.avgstatusesbycity WHERE place = '%s'" % place)
    lengthRow = session.execute("SELECT * FROM twitter.avgtweetlengthbycity WHERE place = '%s'" % place)
    
    try:
        avg_grade = str(gradeRow[0].avg_grade)
    except IndexError:
        avg_grade = "No Data"

    try:
        avg_income = str(incomeRow[0].income)
    except IndexError:
        avg_income = "No Data"

    try:
        population = str(incomeRow[0].pop)
    except IndexError:
        population = "No Data"

    try:  
        avg_followers = str(followersRow[0].avg_followers)
    except IndexError:
        avg_followers = "No Data"

    try:
        avg_statuses = str(statusesRow[0].avg_statuses)
    except IndexError:
        avg_statuses = "No Data"

    try:
        avg_tweet_length = str(lengthRow[0].avg_tweet_length)
    except IndexError:
        avg_tweet_length = "No Data"

    context = {
        'place' : place,
        'avg_grade' : avg_grade,
        'avg_income' : avg_income,
        'population' : population,
        'avg_followers' : avg_followers,
        'avg_statuses' : avg_statuses,
        'avg_tweet_length' : avg_tweet_length,
    }

    return render(request, 'cs179/results.html', context)
