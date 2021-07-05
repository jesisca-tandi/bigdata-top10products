## Packages versions:
# Python 3.7.0 (v3.7.0:1bf9cc5093)
# pyspark 2.4.4 hadoop2.7
# scala 2.11.12
# java jdk1.8.0_221


import re,os,sys,shutil
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


# Load input data
# First argument is path to the reviews data
# Second argument is path to the metadata
reviewDataFile = sys.argv[1] 
metaDataFile = sys.argv[2] 
# reviewDataFile = "reviews_Musical_Instruments_5.json"
# metaDataFile = "metadata.json"


# Initiate spark environment
conf = SparkConf()
sc = SparkContext(conf=conf)




################### Step 1 ###################
### Find the number of unique reviewer IDs ###
### for each product from the review file  ###

# Use read.json function from pyspark.sql to read the data (json)
spark = SparkSession(sc)
dt = spark.read.json(reviewDataFile)

# Select only the required columns from the review data, i.e. product ID (asin) and reviewer ID
dt = dt.select(['asin', 'reviewerID'])

# Convert dataframe to RDD
dt_rdd = dt.rdd.map(tuple)

# Generate key-value pairs where Key: (asin, reviewerID), Value: 1
dt_rdd = dt_rdd.map(lambda w: (w, 1))

# Filter out reviews made by the same reviewerID for the same product (asin)
# using reduce method with key generated above (asin, reviewerID)
# leaving only one (asin, reviewerID) entry
uniques = dt_rdd.reduceByKey(lambda x,y: (x))

# Count the number of (unique) reviews for each product
reviews = uniques.map(lambda w: (w[0][0], 1))
counts = reviews.reduceByKey(lambda n1, n2: n1 + n2)




################### Step 2 ###################
#### Create an RDD, based on the metadata ####
#### Key is the product ID/asin,          ####
#### Value is the product price           ####

# Load json
metadata = spark.read.json(metaDataFile)

# Select only the required columns from the review file, i.e. product ID (asin) and price (price)
metadata = metadata.select(['asin', 'price'])

# Convert metadata to RDD
metadata_rdd = metadata.rdd.map(tuple) # Assume product ID (asin) is unique, no duplicates




################### Step 3 ###################
####      Join number of reviews and      ####
####      price for each product.         ####

# Use inner join to only consider products with both reviews and metadata
combined = counts.join(metadata_rdd).map(lambda x: (x[0], x[1][0], x[1][1]))




################### Step 4 ###################
####  Find top 10 reviewed products and   ####
####  display the product id, unique      ####
####  review counts, and price            ####

top10 = sc.parallelize(combined.takeOrdered(10, key=lambda x: -x[1]))

# Check if directory already exists
outdir = 'output'
if os.path.exists(outdir):
	shutil.rmtree(outdir)

# Save into outdir
top10.saveAsTextFile(outdir)
sc.stop()

