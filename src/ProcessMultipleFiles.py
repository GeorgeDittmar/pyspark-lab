'''
Author: George Dittmar
Date:   04/16/2016

Example of reading in 1 csv file and scrubbing out the ip address contained in the data and then saving the scrubbed data
back out to a new file.
'''

from pyspark import SparkContext

# setup spark context
sc = SparkContext("local", "data_processor")

input = sc.textFile('../resources/csv/*')

# read in csv file, filter out columns we dont want in the new file
# id,first_name,last_name,email,gender,ip_address
header = input.take(1)[0]
headerRDD = sc.parallelize([header])
rows = input.filter(lambda line: line != header)

# helper function to scrub the ip address from the test data. this will be passed into the middle map
def ipScrubber(elm):
    elm[len(elm)-1] = u'0.0.0.0'
    return elm

# filter ip address by changing the form to xxx.xxx.xxx
scrubbed = rows.map(lambda x: x.split(",")).map(lambda x : ipScrubber(x)).map(lambda x : ','.join(x))
print(scrubbed.take(10))

# recombine the header rdd with the scrubbed data rdd
recombined = headerRDD.union(scrubbed)

# write out rdd to directory test.csv
recombined.coalesce(1).saveAsTextFile("test.csv")

# or collect the rdd and write out good old fashioned python way...this could blow the heap though for large RDD's
nonRDD = recombined.toLocalIterator()

f = open('test-file.csv','w')

# write out line by line since nonRDD is an iterator. Append the stupid \n cause python...
for row in nonRDD:
    f.write(row+"\n")

f.close()

