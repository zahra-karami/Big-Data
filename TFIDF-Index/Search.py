#!/usr/bin/env python
# coding: utf-8

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
import json

def generate_TFIDF(sc, df , sqlcontext):
  
	# 1. calculate the number of rows(documents) in data framework
    t_num = df.count()    
    
	# 2. select _id and lower the text_entry and remove punctuation symbols 
	#and then split it as a list of words('tokens')
    word_spilits = df.select("_id",F.split(F.lower(F.regexp_replace(df.text_entry,'[^\w\s]' ,'')),' ').alias('tokens'))
	
	# 3. explode the list of words to generate a list of _id and token
	#then, group the list base on _id and token to calculate frequency of tokens (tf) in each row
	# to create a  data framework words_tf (_id , token , tf)
    words_tf = word_spilits.select("_id", F.explode(word_spilits.tokens).alias('token'))\	
	.groupBy("_id", "token").agg({'token': 'count'}).withColumnRenamed("count(token)", "tf")
	
	# 4. to calculate frequency of token in document (df), I aggregate the list base on token 
	# and created a set of _ids with duplicate _ids eliminated ('collect_set')
	# and calculated the number of _ids and document frequency of a token
	# to create a data framework words_df (_id , token , df)
    words_df = words_tf.groupby("token").agg(F.collect_set("_id").alias("_ids"))\
	.select("token", F.explode("_ids").alias('_id'), F.size("_ids").alias('df'))
    
	# 5. to calculate the final TFIDF data framework, I joined
	# I joined two data frameworks words_tf and words_df base on same _id and token
	# then calculated the idf by fraction of number of documents (t_num) on document frequency (df)
	# then calculated the tf_idf by multiplying idf and tf
    tokensWithTfIdf = words_tf.join(words_df, (words_tf._id == words_df._id) &  (words_tf.token == words_df.token))\
    .select(words_tf._id , words_tf.token, words_tf.tf , words_df.df,(F.log10(t_num / words_df.df )).alias("idf")\
	, (F.log10(t_num / words_df.df ) * words_tf.tf ).alias("tf_idf") )
    
	# 6. cache the TFIDF data framework for further usage 
    tokensWithTfIdf.cache()
    return tokensWithTfIdf

def search_words (query , N ,TFIDF,  df):

	# 1. split the query to words
    query_lst = set(query.lower().split())
    # 2. calculate the number of words in query
	q_n = len(query_lst)
	# 3. search for query words in TFIDF and aggregate base on each document 
	# to summerize tf_idf and calculate the frequency of query words in each document
    search = TFIDF.filter(TFIDF.token.isin(query_lst)).groupby(TFIDF._id)\
	.agg(F.sum("tf_idf").alias("sum_tf_idf"), F.count("tf_idf").alias('freq'))      

	# 4. the score is calculated by multiplying sum_tf_idf to frequency of query words in document 
	# and dividing it to the number of words in query
	# in last step, order the results by highest scores and get N top of them as output
    search = search.select((search._id).alias("id") , (search.sum_tf_idf * search.freq / q_n).alias('scores'))\
	.orderBy("scores", ascending =False).limit(N)
	
	# 5. In the end, join the search output with original data framework to fetch text_entry
	# and select _id , rounded score (3 decimal) and text_entry as result of search
    search = search.join(df, df._id == search.id).select(search.id, F.bround(search.scores,3), "text_entry").orderBy("scores", ascending =False)    
    return search.collect()
    
    

def print_result (query ,result):
	#print search output in format of tuple
    print('Search Term :' , query)
    for i in result:
        print(tuple(i))
    print('-'* 50)
    
def main(sc):

	sqlcontext = SQLContext(sc)

	#load json file from path into the data framework
	path = '/user/maria_dev/zara/shakespeare_full.json'
	df = sqlcontext.read.json(path)
	
	#cache the data framework becuase is used several times during the calculations
	df.cache()

	# this method generate the TFIDF data framework
	TFIDF = generate_TFIDF(sc , df , sqlcontext)


	search1_5 = search_words("to be or not" , 5 , TFIDF , df)
	search2_5 = search_words("so far so" , 5 , TFIDF , df)
	search3_5 = search_words("if you said so" , 5 , TFIDF , df)


	# show outputs
	# print the top 5 of search queries
	print_result("to be or not" , search1_5)
	print_result("so far so" , search2_5 )
	print_result("if you said so" , search3_5)



if __name__  == "__main__":
    conf = SparkConf().setAppName("MyApp")
    sc = SparkContext(conf = conf)
    main(sc)
    sc.stop()






