from pyspark import SparkConf,SparkContext
conf = SparkConf().setAppName("Mukesh")
sc = SparkContext(conf=conf)


input_file = "wasb:///data/diamonds.csv"
rdd1 = sc.textFile(input_file)

print("\n Printitng the content of RDD ")
for i in rdd1.take(2):
    print(i)

print("\n \n grouping by result ")
x = rdd1.groupBy(lambda w:w[1])
print([(k, list(v)) for (k, v) in x.collect()])

print("\n\n Result of groupby key")
z = x.groupByKey()
print(list((j[0], list(j[1])) for j in z.collect()))


print("\n\nGetting the number of partitions ... ")
print(rdd1.getNumPartitions())
print(x.getNumPartitions())
print(z.getNumPartitions())

print("\n\n\n printitng the RDD")
print(z.glom().collect())

for i in z.collect():
    print(i)