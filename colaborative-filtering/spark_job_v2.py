from pyspark.sql.functions import udf
from pyspark.sql import SparkSession, Column
from pyspark.sql.types import *
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.context import SparkContext
from pyspark.mllib.evaluation import RegressionMetrics, RankingMetrics
from pyspark.ml.evaluation import RegressionEvaluator

data_path = '/user/rc12g2/'

# mode can be 'FILE' or 'COMPUTE'
mode = 'COMPUTE' 

def start_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark Action Aggregator") \
        .getOrCreate()
    print("JOB -> started")
    return spark

def read_actions_csv(spark):
    action_schema = StructType([
        StructField("USER_ID", IntegerType(), False),
        StructField("TRACK_ID", FloatType(), False),
        StructField("C_DATE", DateType(), False),
        StructField("LIKED", FloatType(), False),
        StructField("DOWNLOAD", FloatType(), False),
        StructField("PURCHASE", FloatType(), False),
        StructField("C_DATE_COEF", FloatType(), False),
        StructField("DURATION_COEF", FloatType(), False)
    ])
    actions = spark.read.csv(data_path + 'final_actions.csv', schema=action_schema, header=True, sep=",")
    actions.show()
    print("JOB -> read action csv")
    return actions

def aggregate_actions(actions, name):
    actions = actions.withColumn(
        'IMPRESSION', 
        (actions['LIKED'] * 4 + actions['PURCHASE'] * 2 + actions['DOWNLOAD']) * actions['C_DATE_COEF'] * actions['DURATION_COEF']
    )
    print("JOB -> add IMPRESSION to '{}' df".format(name))
    
    df = actions.groupBy([
        'TRACK_ID',
        'USER_ID'
    ]).agg({
        'IMPRESSION': 'sum'
    }).withColumnRenamed(
        'sum(IMPRESSION)', 'IMPRESSION'
    )
    df.show()
    print("JOB -> aggregate actions for '{}' df range".format(name))

    df.printSchema()
    df.write.csv(data_path + '{}-actions-v3'.format(name), header=False)
    print("JOB -> save '{}' df".format(name))

    return df

def read_aggregated_actions(name):
    file_name = data_path + '{}-actions-v3/data.csv'.format(name)
    print("JOB -> start reading '{}' file".format(file_name))
    
    aggregated_actions_schema = StructType([
        StructField("TRACK_ID", FloatType(), False),
        StructField("USER_ID", IntegerType(), False),
        StructField("IMPRESSION", FloatType(), False)
    ])
    df = spark.read.csv(file_name, schema=aggregated_actions_schema, header=False)
    df.printSchema()
    df.show()
    print("JOB -> read '{}' aggregatd action csv".format(name))
    return df

def learn_model(train_actions, implicit=False):
    als = ALS(maxIter=5, regParam=0.15, rank=100, 
            userCol="USER_ID", itemCol="TRACK_ID", ratingCol="IMPRESSION",
            coldStartStrategy="drop", implicitPrefs=implicit)
    model = als.fit(train_actions)
    print("JOB -> implicit={} model -> traind".format(implicit))
    return model

def array_to_string(data):
    return '[' + ','.join([str(elem[0]) for elem in data]) + ']'

def recommend(model):
    array_to_string_udf = udf(array_to_string, StringType())

    user_recs = model.recommendForAllUsers(10)
    print("JOB -> user recommendation list is ready")
    # print("JOB -> num of recommended user: {}".format(user_recs.count()))
    # user_recs.show()
    print("JOB -> converting user recommedation array column")
    user_recs = user_recs.withColumn('RECOMMENDATIONS_STR', array_to_string_udf(user_recs["recommendations"])).drop("recommendations")
    print("JOB -> user recommedation array column converted to string")
    user_recs.write.csv(data_path + 'user_recs_v3', header=False)
    print("JOB -> user recommendations saved")

if __name__ == "__main__":
    spark = start_spark()
    
    if mode == "FILE":
        final_actions = read_aggregated_actions(name='final')
    elif mode == "COMPUTE":
        actions = read_actions_csv(spark)
        final_actions = aggregate_actions(actions, name='final')
    
    model = learn_model(final_actions, implicit=False)
    recommend(model)

    spark.stop()