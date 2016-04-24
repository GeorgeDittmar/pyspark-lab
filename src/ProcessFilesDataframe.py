from pyspark import SparkContext

# setup spark context
sc = SparkContext("local", "data_processor")

input = sc.textFile('../resources/csv/set1.csv')
