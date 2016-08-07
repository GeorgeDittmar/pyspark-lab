from pyspark import SparkContext
from pyspark.sql import SQLContext

# setup spark context
from pyspark.sql.types import StructType, StructField, StringType

sc = SparkContext("local", "data_processor")
sqlC = SQLContext(sc)
# create dummy data frames

rdd1 = sc.range(0,10000000).map(lambda x: ("key "+str(x), x)).repartition(100)
rdd2 = sc.range(0,10000).map(lambda x: ("key "+str(x), x)).repartition(10)



# Define schema
schema = StructType([
    StructField("Id", StringType(), True),
    StructField("Packsize", StringType(), True)
])

schema2 = StructType([
    StructField("Id2", StringType(), True),
    StructField("Packsize", StringType(), True)
])

df1 = sqlC.createDataFrame(rdd1,schema)
df2 = sqlC.createDataFrame(rdd2,schema2)

print df1.rdd.getNumPartitions()
print df2.rdd.getNumPartitions()
# df1p1 = df1.repartition(1)
# df2p1 = df2.repartition(1)
print 'STARTING JOIN'

df3 = df1.join(df2, df1.Id == df2.Id2).collect()
print 'DONE'

print

print 'Map side join'

rdd2Map = sc.broadcast(rdd2.collectAsMap())
print 'STARTING'
joinedRDD = rdd1.map(lambda (k,v): (k,v, rdd2Map.value[k]) if k in rdd2Map.value else (k,v)).filter(lambda x: len(x) == 3).collect()
print 'END'

print joinedRDD[:100]

print len(df3)
print len(joinedRDD)