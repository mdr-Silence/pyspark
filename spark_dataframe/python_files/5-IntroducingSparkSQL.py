
# coding: utf-8

# ### Analyzing airline data with Spark SQL

# In[5]:


from pyspark.sql import SparkSession

spark = SparkSession.builder\
                    .appName("Analyzing airline data")\
                    .getOrCreate()


# ### Exploring SQL query options

# In[1]:


from pyspark.sql.types import Row
from datetime import datetime


# #### Creating a dataframe with different data types

# In[23]:


record = sc.parallelize([Row(id = 1,
                             name = "Jill",
                             active = True,
                             clubs = ['chess', 'hockey'],
                             subjects = {"math": 80, 'english': 56},
                             enrolled = datetime(2014, 8, 1, 14, 1, 5)),
                         Row(id = 2,
                             name = "George",
                             active = False,
                             clubs = ['chess', 'soccer'],
                             subjects = {"math": 60, 'english': 96},
                             enrolled = datetime(2015, 3, 21, 8, 2, 5))
])


# In[24]:


record_df = record.toDF()
record_df.show()


# #### Register the dataframe as a temporary view
# 
# * The view is valid for one session
# * This is required to run SQL commands on the dataframe

# In[25]:


record_df.createOrReplaceTempView("records")


# In[26]:


all_records_df = sqlContext.sql('SELECT * FROM records')

all_records_df.show()


# In[27]:


sqlContext.sql('SELECT id, clubs[1], subjects["english"] FROM records').show()


# In[28]:


sqlContext.sql('SELECT id, NOT active FROM records').show()


# ### Conditional statements in SQL 

# In[29]:


sqlContext.sql('SELECT * FROM records where active').show()


# In[30]:


sqlContext.sql('SELECT * FROM records where subjects["english"] > 90').show()


# #### Global temporary view
# 
# * Temporary view shared across multiple sessions
# * Kept alive till the Spark application terminates

# In[32]:


record_df.createGlobalTempView("global_records")


# In[35]:


sqlContext.sql('SELECT * FROM global_temp.global_records').show()


# ## Working on external dataset with SQL queries

# Data Collection

# In[21]:


airlinesPath = "projects/spark2/demo/datasets/airlines.csv"
flightsPath = "projects/spark2/demo/datasets/flights.csv"
airportsPath = "projects/spark2/demo/datasets/airports.csv"


# Loading External Data As DataFrames

# In[22]:


airlines = spark.read\
                .format("csv")\
                .option("header", "true")\
                .load(airlinesPath)


# In[23]:


airlines.createOrReplaceTempView("airlines")


# In[24]:


df = spark.sql("SELECT * FROM airlines")
df.show(5)


# In[25]:


flights = spark.read\
               .format("csv")\
               .option("header", "true")\
               .load(flightsPath)


# In[26]:


flights.createOrReplaceTempView("flights")


# In[27]:


flights.columns


# ### Using count function with python commands

# In[28]:


flights_count_py = flights.count()
airlines_count_py = airlines.count()


# In[29]:


flights_count_py, airlines_count_py


# ### Using count function with SQL query

# In[30]:


flights_count = spark.sql("SELECT COUNT(*) FROM flights")
airlines_count = spark.sql("SELECT COUNT(*) FROM airlines")


# **NOTE: Count( ) Using SQL Query Doesnt Return Integer, It Returns DataFrame **

# In[31]:


flights_count, airlines_count


# In[32]:


flights_count.collect()[0][0], airlines_count.collect()[0][0]


# In[33]:


flights_count = flights_count.collect()[0][0]
airlines_count = airlines_count.collect()[0][0]


# ### Finding average distance of flights

# Summation Of All The Distance

# In[34]:


total_distance_DF = spark.sql("SELECT distance FROM flights")\
                         .agg({"distance":"sum"})\
                         .withColumnRenamed("sum(distance)","total_distance")


# In[35]:


total_distance_DF.show()


# In[36]:


total_distance = total_distance_DF.collect()[0][0] 


# Finding Average Distance

# In[37]:


avg_distance = total_distance/flights.count()
avg_distance


# ### How many flights got delayed

# Fligts With Delay

# In[38]:


all_delay = spark.sql("select airlines, flight_number, departure_delay from flights where departure_delay > 0")


# In[39]:


all_delay.createOrReplaceTempView("all_delay")


# In[40]:


all_delay.orderBy(all_delay.departure_delay.desc()).show(5)


# Total Number Of Flights Got Delayed

# In[41]:


delay_count = spark.sql("SELECT COUNT(departure_delay) FROM all_delay")


# In[42]:


delay_count.show()


# In[43]:


delay_count.collect()[0][0]


# ### How many percent flights got delayed

# In[44]:


delay_percent = delay_count.collect()[0][0] / flights_count * 100
delay_percent


# ### Finding delay per aIrlines

# In[45]:


delay_per_airline = spark.sql("SELECT airlines, departure_delay FROM flights")\
                         .groupBy("airlines")\
                         .agg({"departure_delay":"avg"})\
                         .withColumnRenamed("avg(departure_delay)","departure_delay")


# In[46]:


delay_per_airline.orderBy(delay_per_airline.departure_delay.desc()).show(5)


# In[47]:


delay_per_airline.createOrReplaceTempView("delay_per_airline")


# In[48]:


delay_per_airline = spark.sql("SELECT * FROM delay_per_airline ORDER BY departure_delay DESC")


# In[49]:


delay_per_airline.show(5)


# ## Join Operation in SQL Commands

# Now we know the number of airlines which have delay but we dont know thier names<br>
# We need to get their names from different DataFrame

# In[50]:


spark.sql("SELECT * FROM delay_per_airline JOIN airlines ON airlines.code = delay_per_airline.airlines").show(5)


# **Note: We are getting duplicate columns with different names** <br>
#       For this we need to drop unwanted column

# In[51]:


spark.sql("SELECT * FROM delay_per_airline JOIN airlines ON airlines.code = delay_per_airline.airlines")\
     .drop("code")\
     .show(5)


# * TODO: How many flights got delayed per airport
# * Find the airports with the worst and best delays
# * Average distance a flight travels from an airport

# ### Finding average distance travelled by flights per airport

# In[52]:


#Loading data
airports = spark.read\
                .format("csv")\
                .option("header", "true")\
                .load(airportsPath) 


# In[53]:


#Creating Temp view
airports.createOrReplaceTempView("airports") 


# In[54]:


dist_airport = spark.sql("SELECT origin,distance FROM flights")\
                    .groupBy("origin")\
                    .agg({"distance":"avg"})\
                    .withColumnRenamed("avg(distance)","distance")


# In[55]:


dist_airport.createOrReplaceTempView("dist_airport")


# In[56]:


spark.sql("SELECT * FROM dist_airport ORDER BY distance DESC").show(5)


# In[57]:


spark.sql("SELECT * FROM dist_airport JOIN airports ON airports.code = dist_airport.origin ORDER BY distance DESC")\
     .drop('origin')\
     .show(5)


# ### How many Flights got Delayed per Airport 

# TODO
# * add percent

# In[58]:


total_delay = spark.sql("SELECT origin, departure_delay FROM flights where departure_delay > 0")
                   .groupBy("origin")\
                   .agg({"departure_delay":"sum"})\
                   .withColumnRenamed("sum(departure_delay)","departure_delay")


# In[59]:


import pyspark.sql.functions as func


# In[60]:


total = flights.agg({"departure_delay":"sum"}).collect()[0][0]


# In[61]:


total_delay = total_delay.withColumn("percent",
                                     func.round(total_delay['departure_delay']/total * 100, 2))


# In[62]:


total_delay.createOrReplaceTempView("total_delay")


# In[63]:


spark.sql("SELECT * FROM total_delay ORDER BY percent DESC").show(5)


# In[64]:


spark.sql("SELECT * FROM total_delay ORDER BY percent ASC").show(5)


# ### Average Delays of Flight  from Airport

# In[65]:


import pyspark.sql.functions as func


# In[66]:


delays_airport = spark.sql("SELECT origin, departure_delay FROM flights where departure_delay > 0")\
                      .groupBy("origin")\
                      .agg(func.avg('departure_delay')\
                      .alias('departure_delay'))


# In[67]:


delays_airport.show(5)


# In[68]:


delays_airport.createOrReplaceTempView("delays_airport")


# In[69]:


delays_airport = spark.sql("SELECT * FROM delays_airport ORDER BY departure_delay DESC")
delays_airport.show(5)

