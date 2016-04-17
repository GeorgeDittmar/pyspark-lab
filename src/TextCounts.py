'''
Author: George Dittmar
Date:   04/16/2016

Example of reading in text, precprocessing it, then performing a basic term count / term frequency calculation on the
data.
'''
from pyspark import SparkContext

# setup spark context
sc = SparkContext("local", "Simple App")


chapter = sc.textFile('../resources/sample.txt').cache()

# remember that spark is immutable by default so remember that one transformation results in a new RDD of information
# this "one" liner reads in the sample.txt file, preprocesses the text then gets the word counts for the text.
splitText = chapter.map( lambda x: x.replace(',',' ').replace('.',' ').replace('-',' ').lower())\
    .flatMap(lambda x : x.split())\
    .map(lambda x: (x,1))\
    .reduceByKey(lambda x,y: x+y).sortBy(lambda (k,v): v,False).cache()

# get the total number of words in the document
total = splitText.map(lambda (k,v) : v).reduce(lambda x,y : x+y )

# calculate the term frequency in the sample text so we can get some sweet sweet term frequency stats!
termFrequency = splitText.map(lambda x: (x[0],x[1]/float(total))).sortBy(lambda (k,v): v,False)

# the magic of Spark!!!
# Whats really magical is that none of the above code is executed until when we want to print out to console!
print("Total number of words: "+str(total)+"\n")
print("Top 10 most frequent terms: "+str(termFrequency.take(10)))