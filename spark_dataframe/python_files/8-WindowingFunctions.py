
# coding: utf-8

# ### Window functions

# In[1]:


from pyspark.sql import SparkSession

spark = SparkSession.builder\
                    .appName("Window functions")\
                    .getOrCreate()


# In[2]:


products = spark.read\
                .format("csv")\
                .option("header", "true")\
                .load('/projects/spark2/demo/datasets/products.csv')


# In[4]:


products.show()


# #### Window rank function

# In[21]:


import sys
from pyspark.sql.window import Window
import pyspark.sql.functions as func


# In[23]:


windowSpec1 = Window.partitionBy(products['category'])\
                    .orderBy(products['price'].desc())


# In[24]:


price_rank = (func.rank().over(windowSpec1))


# In[31]:


product_rank = products.select(
        products['product'],
        products['category'],
        products['price']
).withColumn('rank', func.rank().over(windowSpec1))

product_rank.show()


# #### Window max function between rows

# In[47]:


windowSpec2 = Window.partitionBy(products['category'])\
                    .orderBy(products['price'].desc())\
                    .rowsBetween(-1, 0)


# In[48]:


price_max = (func.max(products['price']).over(windowSpec2))


# In[49]:


products.select(
    products['product'],
    products['category'],
    products['price'],
    price_max.alias("price_max")).show()


# #### Window price difference function between ranges

# In[53]:


windowSpec3 = Window.partitionBy(products['category'])\
                    .orderBy(products['price'].desc())\
                    .rangeBetween(-sys.maxsize, sys.maxsize)


# In[54]:


price_difference =   (func.max(products['price']).over(windowSpec3) - products['price'])


# In[55]:


products.select(
    products['product'],
    products['category'],
    products['price'],
    price_difference.alias("price_difference")).show()

