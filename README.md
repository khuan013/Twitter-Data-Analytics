Twitter Data Analysis
====

Overview
------

Data analytic tool for processing tweets after collection with the twitter streaming API, and visualizing the information using the Google Maps API

Can be used to find the following for each city in the US

* Average grade level determined by textstat
* Trending hashtags
* Most common keywords
* Most linked web domains
* Average tweet length
* Average number of followers

In addition, we aggregated 2010 census data for average household income and population size to compare the data.

Language(s)
---

Python, HTML, Javascript

Dependencies
---

Apache Spark Python API

* Engine for large-scale data processing. Used to analyze and preprocess our data in a distributed way

Cassandra 2.0.17
 
 * High performance distributed database. Used to store information processed by Spark.
 
 Textstat
 
 * Calculates statistics from text. Used to determine the average grade level of a tweet, which is later averaged by city.
 
 Screenshots
 ---
 
 ![alt text](http://i.imgur.com/ei41sZ6.png)
 
 ![alt text](http://i.imgur.com/2CWv8tl.png)
 
 
