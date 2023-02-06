"""
    Spark app 
"""


import sys
import requests
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

### MY IMPORTS
# import time
from datetime import datetime

def aggregate_count(new_values, total_sum):
	  return sum(new_values) + (total_sum or 0)
    
# def aggregate_repos(new_dates, old_dates):
#     return old_dates or new_dates #keeps old date

# def aggregate_repos(new_dates, old_dates):
#     if new_dates == None and old_dates != None:
#         return old_dates
#     elif old_dates == None and new_dates != None:
#         return new_dates
#     return min(new_dates, old_dates) #keeps old date

# map(lambda w: (w[1], (w[2], w[0], w[3])))#((full_name) , (pushed_at, language, stargazer))

def aggregate_repos(new_values, old_values):
    #format: (key:full_name , value:(pushed_at, language, stargazer))
    new_dates = None
    old_dates = None
    # print(new_values)
    # print(old_values)
    if new_values != None and len(new_values) > 0:
        new_dates = new_values[0] #this is pushed_at
    
    if old_values != None and len(old_values) > 0:
        old_dates = old_values[0] #pushed_at

    if new_dates == None and old_dates != None:
        return old_values
    elif old_dates == None and new_dates != None:
        return new_values
    else: #both have data
        if new_dates < old_dates:
            return new_values
        else:
            return old_values
    # return min(new_dates, old_dates) #keeps old date

def get_sql_context_instance(spark_context):
    if('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SparkSession(spark_context)
    return globals()['sqlContextSingletonInstance']

def __datetime(date_str):
    return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')

# def send_df_to_dashboard(df):
#     url = 'http://webapp:5000/updateData'
#     data = df.toPandas().to_dict('list')
#     requests.post(url, json=data)

# batchCountData = None

def send_df_to_dashboard(df):
    url = 'http://webapp:5000/updateData'
    requests.post(url, json=df)

# def send_df_to_dashboard():
#     url = 'http://webapp:5000/updateData'
#     # if not df.isEmpty():
#     data = {}
#     print(batchCountData)
#     data['batchCountData'] = batchCountData
#     requests.post(url, json=data)


def set_df_to_global_batchCount(df):
    url = 'http://webapp:5000/updateBatchData'
    if not df.isEmpty():
        data = df.toDF().toJSON().collect()
        # batchCountData = df.toDF().toJSON().collect()
        requests.post(url, json=data)

# def process_rdd(time, rdd):
#     pass
#     print("----------- %s -----------" % str(time))
#     try:
#         sql_context = get_sql_context_instance(rdd.context)
#         row_rdd = rdd.map(lambda w: Row(isMultipleOf9=w[0], count=w[1]))
#         results_df = sql_context.createDataFrame(row_rdd)
#         results_df.createOrReplaceTempView("results")
#         new_results_df = sql_context.sql("select isMultipleOf9, count from results order by count")
#         new_results_df.show()
#         send_df_to_dashboard(new_results_df)
#     except ValueError:
#         print("Waiting for data...")
#     except:
#         e = sys.exc_info()[0]
#         print("Error: %s" % e)

##########################################
def process_rdd(time, rdd):
    try:
        sql_context = get_sql_context_instance(rdd.context)

        # This will be sent
        data_to_send = {}

        #format: (key:full_name , value:(pushed_at, language, stargazer))
        # print(rdd.take(5))
        row_rdd = rdd.map(lambda w: Row(full_name=w[0], language=w[1][0][1], stargazers_count=int(w[1][0][2]), pushed_at=w[1][0][0])) #mine # desc=w[1][0][3]
        

        # print(row_rdd.take(150))

        # print(row_rdd.take(5))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("results")

        # TESTING
        # new_results_df = sql_context.sql("select stargazers_count as avg_python_stargazer from results where language = 'PYTHON'")
        # new_results_df.show()
        # new_results_df = sql_context.sql("select stargazers_count as avg_python_stargazer from results where language = 'CSHARP'")
        # new_results_df.show()
        # new_results_df = sql_context.sql("select stargazers_count as avg_python_stargazer from results where language = 'JAVA'")
        # new_results_df.show()

        # new_results_df = sql_context.sql("select count( distinct full_name) from results")
        # new_results_df.show()
        

        #get 
        new_results_df = sql_context.sql("select count( distinct full_name) as total_python_repos from results where language = 'PYTHON'")
        new_results_df.show()
        data_to_send['i_python'] = new_results_df.toPandas().to_dict('list')

        new_results_df = sql_context.sql("select count( distinct full_name) as total_csharp_repos from results where language = 'CSHARP'")
        new_results_df.show()
        data_to_send['i_csharp'] = new_results_df.toPandas().to_dict('list')

        new_results_df = sql_context.sql("select count( distinct full_name) as total_java_repos from results where language = 'JAVA'")
        new_results_df.show()
        data_to_send['i_java'] = new_results_df.toPandas().to_dict('list')
        

        ####(full_name , (pushed_at, language, stargazer))
        new_results_df = sql_context.sql("select avg ( stargazers_count) as avg_python_stargazer from results where language = 'PYTHON'")
        new_results_df.show()
        data_to_send['iii_python'] = new_results_df.toPandas().to_dict('list')

        new_results_df = sql_context.sql("select avg ( stargazers_count) as avg_csharp_stargazer from results where language = 'CSHARP'")
        new_results_df.show()
        data_to_send['iii_csharp'] = new_results_df.toPandas().to_dict('list')

        new_results_df = sql_context.sql("select avg ( stargazers_count) as avg_java_stargazer from results where language = 'JAVA'")
        new_results_df.show()
        data_to_send['iii_java'] = new_results_df.toPandas().to_dict('list')

        ################
        row_Descrdd = rdd.filter(lambda item: item[1][0][1] == 'PYTHON').flatMap(lambda w: w[1][0][3].split(" ")).filter(lambda d: d != '').map(lambda x: (x, 1)).reduceByKey(lambda a, b: a+b)

        row_DescrddROW = row_Descrdd.map(lambda w: Row(word=w[0], frequency=w[1]))

        results_df = sql_context.createDataFrame(row_DescrddROW)
        results_df.createOrReplaceTempView("pythondescresults")

        # new_results_df = sql_context.sql("select count( distinct full_name) from descresults")
        new_results_df = sql_context.sql("select word as python_words, frequency from pythondescresults order by frequency desc limit 10")
        new_results_df.show()
        data_to_send['iv_python'] = new_results_df.toPandas().to_dict('list')
        #################
        row_Descrdd = rdd.filter(lambda item: item[1][0][1] == 'CSHARP').flatMap(lambda w: w[1][0][3].split(" ")).filter(lambda d: d != '').map(lambda x: (x, 1)).reduceByKey(lambda a, b: a+b)

        row_DescrddROW = row_Descrdd.map(lambda w: Row(word=w[0], frequency=w[1]))

        results_df = sql_context.createDataFrame(row_DescrddROW)
        results_df.createOrReplaceTempView("csharpdescresults")

        # new_results_df = sql_context.sql("select count( distinct full_name) from descresults")
        new_results_df = sql_context.sql("select word as csharp_words, frequency from csharpdescresults order by frequency desc limit 10")
        new_results_df.show()
        data_to_send['iv_csharp'] = new_results_df.toPandas().to_dict('list')
        ####################
        row_Descrdd = rdd.filter(lambda item: item[1][0][1] == 'JAVA').flatMap(lambda w: w[1][0][3].split(" ")).filter(lambda d: d != '').map(lambda x: (x, 1)).reduceByKey(lambda a, b: a+b)

        row_DescrddROW = row_Descrdd.map(lambda w: Row(word=w[0], frequency=w[1]))

        results_df = sql_context.createDataFrame(row_DescrddROW)
        results_df.createOrReplaceTempView("javadescresults")

        # new_results_df = sql_context.sql("select count( distinct full_name) from descresults")
        new_results_df = sql_context.sql("select word as java_words, frequency from javadescresults order by frequency desc limit 10")
        new_results_df.show()
        data_to_send['iv_java'] = new_results_df.toPandas().to_dict('list')
        #####################


        send_df_to_dashboard(data_to_send)
        ####################
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


################################
def process_rddDesc(time, rdd):
    try:
        sql_context = get_sql_context_instance(rdd.context)
        # row_rdd = rdd.map(lambda w: Row(isMultipleOf9=w[0], count=w[1]))
        row_rdd = rdd.map(lambda w: Row(word=w[0], frequency=w[1])) #mine

        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("descresults")

        new_results_df = sql_context.sql("select word, frequency from descresults order by frequency desc limit 10")

        new_results_df.show()
        # send_df_to_dashboard(new_results_df)
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

# isDataStreaming = False

if __name__ == "__main__":
    DATA_SOURCE_IP = "data-source" #orig
    DATA_SOURCE_PORT = 9999 #orig
    # sc = SparkContext(appName="NineMultiples") #orig
    sc = SparkContext(appName="Streaming")
    sc.setLogLevel("ERROR") #orig
    # ssc = StreamingContext(sc, 2) #orig
    ssc = StreamingContext(sc, 60) #orig
    # ssc.checkpoint("checkpoint_NineMultiples") #orig
    ssc.checkpoint("checkpoint_Streaming")
    data = ssc.socketTextStream(DATA_SOURCE_IP, DATA_SOURCE_PORT) #orig

    print("Connection Established...")


    newData = data.map(lambda repo: repo.split("\t:")) # SPLITS DATA
    
    repoData = newData.map(lambda w: (w[1], (w[2], w[0], int(w[3]), w[4])))#(full_name , (pushed_at, language, stargazer, desc))
    
    repoCount = repoData.updateStateByKey(aggregate_repos) ### REDUCE BY KEY BASED ON STATE
    repoCount.foreachRDD(process_rdd) ### PROCESS EACH RDD
    ################

    ######################
    currentUTCtime = datetime.utcnow() #because github is in utc format
    # batchRepos = newData.filter(lambda w: (currentUTCtime - __datetime(w[2])).total_seconds() <= 60).map(lambda x: (x[1],(x[2], x[0]))).reduceByKey(lambda x, y:  min(x[0],y[0])) #name, (pushed, lang)
    batchRepos = newData.filter(lambda w: (currentUTCtime - __datetime(w[2])).total_seconds() <= 60).map(lambda x: (x[1],(x[2], x[0]))).reduceByKey(lambda x, y:  aggregate_repos(x,y)) #name, (pushed, lang)
    # batchRepos.pprint()
    batchCountRepos = batchRepos.map(lambda w: (w[1][1], 1)).reduceByKey(lambda a, b: a+b) #(lang, 1) -> reducebykey on language, frequency (by adding new value with old)
    batchCountRepos.pprint()
    
    batchCountRepos.foreachRDD(lambda x: set_df_to_global_batchCount(x)) ### SEND TO DASHBOARD


    ssc.start() #orig
    ssc.awaitTermination() #orig


