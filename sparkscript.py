from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import udf, col, size
from pyspark_cassandra import CassandraSparkContext
 
from textstat.textstat import textstat
from stop_words import get_stop_words
from topia.termextract import extract
 
import tldextract
import operator
import os
'''
Spark test script
 
- How to run:
	spark-submit --master local[4] test.py
 
- contains examples to learn how to use the Spark Python API
 
'''
 
 
stop_words = get_stop_words('english')
stop_words.append("https ://t")
extractor = extract.TermExtractor()
extractor.filter = extract.DefaultFilter()
if __name__ == "__main__":
	# command line arguments?
	# Spark configuration
	conf = SparkConf().setAppName("Tweet Data by City")
	sc = CassandraSparkContext(conf=conf)
	sqlContext = SQLContext(sc)
 
	# read files
	rawTweets = sqlContext.read.json("../tweets.json")
	
	# split the data into two sets
	# rawTweets.geo is of type 'Column', which has the isNotNull() and isNull() functions to check for nullity
	geoTweets = rawTweets.filter(rawTweets.geo.isNotNull())
	placeTweets = rawTweets.filter(rawTweets.geo.isNull())
 
	# geo tweet processing
 
	# place tweet processing
	# placeTweets is a DataFrame which has the filter() function
	# filter takes a condition using a column, or a SQL expression in string form, here I use the column condition method
	placeTweets = placeTweets.filter(placeTweets.place.country == "United States")
	
	# example of processing the tweet text
	def processTweets(text):
		asciiTweet = text.encode('ascii', 'ignore')
		if textstat.lexicon_count(asciiTweet) == 0:
			return 0
		else:
			return textstat.flesch_kincaid_grade(asciiTweet)
	
	# make a new data frame with grade data
	# make a new function we can use with the withColumn function
	udfGrade = udf(processTweets, FloatType())
	gradeTweets = placeTweets.withColumn("grade", udfGrade("text"))
	
	# group and aggregate the data
	avgGradeByCity = gradeTweets.groupBy("place.full_name").agg({"grade" : "avg"})
        temp = avgGradeByCity.map(lambda row: {'place' : row.full_name,
                                               'avg_grade' : row["avg(grade)"]}).collect()
        sc.parallelize(temp).saveToCassandra(keyspace='twitter', table='avggradebycity')
 
	def wordCount(row):
	    # re-encode the tweet, and count the words
	    wordDict = dict()
	    tweet = row.text.encode('ascii', 'ignore').lower()
	    arrayTuples = extractor(tweet)
	    for element in arrayTuples:
		if element[0] not in wordDict and element[0] not in stop_words:
		    wordDict[element[0]] = element[1]
		elif element[0] not in stop_words:
		    wordDict[element[0]] += element[1]
 
 
	    # form the list of tuples
	    wordFreq = list()
	    for word in wordDict.keys():
		wordFreq.append((word, wordDict[word]))
	
	    #sort
 
 
	    # return in (place, [(word, count), (word, count), ...]) form
	    return (row.place.full_name, wordFreq)
        def hashCount(row):
            wordDict = dict()
            tweet = row.text.encode('ascii', 'ignore').lower()
            words = tweet.split(' ')
            for word in words:
                if len(word) == 0:
                    pass
                elif word[0] == "#" and word[0] not in wordDict:
                    wordDict[word[1:]] = 1
                elif word[0] == "#" and word[0] in wordDict:
                    wordDict[word[1:]] += 1
            
            wordFreq = list()
            for word in wordDict.keys():
                wordFreq.append((word,wordDict[word]))
        
            return (row.place.full_name,wordFreq)
        wcTweets = placeTweets.map(wordCount)
        hashTweets = placeTweets.map(hashCount)
	
        def aggCat(list1, list2):
	    # aggregate and concatenate the word lists
	    wordDict = dict()
	    for tup in list1 + list2:
		if tup[0] not in wordDict:
		    wordDict[tup[0]] = tup[1]
		else:
		    wordDict[tup[0]] += tup[1]
 
	    # form the list of tuples
	    wordFreq = list()
	    for word in wordDict.keys():
		wordFreq.append((word, wordDict[word]))
 
	    wordFreq.sort(key=operator.itemgetter(1), reverse=True)
	    # return a list of tuples this time
	    return wordFreq
 
	# do the actual work
	keywordsByCity = wcTweets.reduceByKey(aggCat)
        
	hashtagsByCity = hashTweets.reduceByKey(aggCat)
        def finalKeywords(row):
            result = dict()
            result["place"] = str(row[0])
 
            wordList = list()
            freqList = list()
 
            if len(row[1]) == 0:
                wordList.append("")
                freqList.append(0)
            else:
                for tup in row[1]:
                    wordList.append(str(tup[0]))
                    freqList.append(int(tup[1]))
 
            result["wordlist"] = wordList
            result["freqlist"] = freqList
 
            return result
 
        def finalHashtags(row):
            result = dict()
            result["place"] = str(row[0])
 
            hashList = list()
            freqList = list()
 
            if len(row[1]) == 0:
                hashList.append("")
                freqList.append(0)
            else:
                for tup in row[1]:
                    hashList.append(str(tup[0]))
                    freqList.append(int(tup[1]))
 
            result["hashlist"] = hashList
            result["freqlist"] = freqList
 
            return result
 
        # save the keywords data to Cassandra
        temp = keywordsByCity.map(finalKeywords).collect()
        sc.parallelize(temp).saveToCassandra(keyspace='twitter', table='keywords')
        temp = hashtagsByCity.map(finalHashtags).collect()
        sc.parallelize(temp).saveToCassandra(keyspace='twitter', table='hashtags')
	
	hashtagsByCity = wcTweets.reduceByKey(aggCat).collect()
	# statistics
	def tweetLength(text):
		return len(text.encode('ascii', 'ignore'))
	
	udfTweetLength = udf(tweetLength, IntegerType())
	tweetsWithLength = placeTweets.withColumn("text_length", udfTweetLength("text"))
	placeGrouped = tweetsWithLength.groupBy("place.full_name")
 
	avgTweetLengthByCity = placeGrouped.agg({"text_length" : "avg"})
	avgFollowersByCity = placeGrouped.agg({"user.followers_count" : "avg"})
	avgStatusesByCity = placeGrouped.agg({"user.statuses_count" : "avg"})
	
        
        #print(avgStatusesByCity.toDF("place" , "status").collect()[0])
        temp = avgTweetLengthByCity.map(lambda row: {'place' : row.full_name,
                                               'avg_tweet_length' : row["avg(text_length)"]}).collect()
        
        sc.parallelize(temp).saveToCassandra(keyspace='twitter', table='avgtweetlengthbycity')
        temp = avgFollowersByCity.toDF("place" , "followers").map(lambda row: {'place' : row.place,
                                               'avg_followers' : row.followers}).collect()
        sc.parallelize(temp).saveToCassandra(keyspace='twitter', table='avgfollowersbycity')
        temp = avgStatusesByCity.toDF("place" , "status").map(lambda row: {'place' : row.place,
                                               'avg_statuses' : row.status}).collect()
        sc.parallelize(temp).saveToCassandra(keyspace='twitter', table='avgstatusesbycity')
 
        urlTweets = placeTweets.filter(size(col("entities.urls")) > 0)
 
 
 
        def domainCount(row):
            domainDict = dict()
            #['entities']['urls'][0]['expanded_url'
            url = row.entities.urls[0].expanded_url;
            ext = tldextract.extract(url)
            domain = ext.domain
            if domain not in domainDict:
                domainDict[domain] = 1
            else:
                domainDict[domain] += 1
 
            urlFreq = list()
 
            for link in domainDict.keys():
                urlFreq.append((link, domainDict[link]))
 
            return(row.place.full_name, urlFreq)
        
        domainTweets = urlTweets.map(domainCount)
 
 
        domainsByCity = domainTweets.reduceByKey(aggCat)
        # save the keywords data to Cassandra
        temp = domainsByCity.map(finalKeywords).collect()
        
        
 
         #   print domainsByCity[i]
#
        #temp.showSchema()
 
        sc.parallelize(temp).saveToCassandra(keyspace='twitter', table='domainsbycity')
