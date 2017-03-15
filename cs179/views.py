from django.shortcuts import render
from django.http import HttpResponse
from django.shortcuts import render_to_response


from cassandra.cluster import Cluster
cluster = Cluster(port=9043)
session = cluster.connect()

# Create your views here.
def index(request):
    data = session.execute("SELECT * FROM twitter.avgincomebystate")
    lst = [['State', 'Income']]
    print data[0]
   
    for i in range(0, 47):
        curr = []
        curr.append(("US-" + data[i].state).encode('ascii','ignore'))
        curr.append(data[i].income)
        lst.append(curr)

    #print lst
   

    data = session.execute("SELECT * FROM twitter.avgincomebycity")
    citylst = []

    
    

    
    for i in range(0, 22157):
        curr = []
        curr.append((data[i].place).encode('ascii','ignore'))
        curr.append(data[i].income)
        curr.append(data[i].pop)
        citylst.append(curr)

    
    twittercity = []

    data = session.execute("SELECT * FROM twitter.avggradebycity")

    for i in range(0, 25301):
        if len(data[i].place) > 4:
            if data[i].place[-4] == ',' and data[i].avg_grade != None:

                curr = []
                curr.append((data[i].place).encode('ascii','ignore'))
                curr.append(data[i].avg_grade)
                twittercity.append(curr)
    
    
    
    
    return render_to_response('cs179/index.html', {'statedata' : lst, 'citydata' : citylst, 'twitterdata': twittercity})

    #return render(request, 'cs179/index.html')

    



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
    domainsRow = session.execute("SELECT * FROM twitter.domainsbycity WHERE place = '%s'" % place)
    
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

    try:
        domainList = domainsRow[0].wordlist[:10]
        domainFreqList = domainsRow[0].freqlist[:10]
        domainHybridList = list()
        for i in range(len(domainList)):
            d = domainList[i] + " -- " + str(domainFreqList[i])
            print(d)
            domainHybridList.append(d)
    except IndexError:
        domainHybridList = ["No Data -- 0"]

    context = {
        'place' : place,
        'avg_grade' : avg_grade,
        'avg_income' : avg_income,
        'population' : population,
        'avg_followers' : avg_followers,
        'avg_statuses' : avg_statuses,
        'avg_tweet_length' : avg_tweet_length,
        'domainHybridList' : domainHybridList,
    }

    return render(request, 'cs179/results.html', context)
