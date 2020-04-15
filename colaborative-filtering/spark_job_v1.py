from pyspark.sql.functions import udf
from pyspark.sql import SparkSession, Column
from pyspark.sql.types import *
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.context import SparkContext
from pyspark.mllib.evaluation import RegressionMetrics, RankingMetrics
from pyspark.ml.evaluation import RegressionEvaluator

data_path = '/user/rc12g2/'

train_start_date = '2019-01-01'
train_end_date = '2019-12-31'

test_start_date = '2020-01-01'
test_end_date = '2020-12-31'

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
        StructField("PUBLISH_DATE", DateType(), False),
        StructField("LIKED", FloatType(), False),
        StructField("DOWNLOAD", FloatType(), False),
        StructField("PURCHASE", FloatType(), False)
    ])
    actions = spark.read.csv(data_path + 'actions-with-time.csv', schema=action_schema, header=True, sep=",")
    actions.show()
    print("JOB -> read action csv")
    return actions

def read_minimal_content_based_csv(spark):
    minimal_content_based = spark.read.csv(data_path + 'minimal_content_based.csv', inferSchema=True, header=True, sep=",")
    # minimal_content_based = spark.read.csv(data_path + 'N_PRICE.csv', inferSchema=True, header=True, sep=",")
    #minimal_content_based = spark.read.csv(data_path + 'N_DURATION.csv', inferSchema=True, header=True, sep=",")
    minimal_content_based.show()
    print("JOB -> read minimal content based csv")
    return minimal_content_based

def aggregate_actions(actions, minimal_content_based, start_date, end_date, name):
    # TODO: (c_date to published_date), (c_date to now())
    df = actions.filter(
        (actions['C_DATE'] >= start_date) & (actions['C_DATE'] <= end_date)
    ).groupBy([
        'TRACK_ID',
        'USER_ID'
    ]).agg({
        'LIKED': 'sum',
        'DOWNLOAD': 'sum',
        'PURCHASE': 'sum'
    }).withColumnRenamed(
        'sum(LIKED)', 'LIKED'
    ).withColumnRenamed(
        'sum(DOWNLOAD)', 'DOWNLOAD'
    ).withColumnRenamed(
        'sum(PURCHASE)', 'PURCHASE'
    )
    df.show()
    print("JOB -> aggregate actions for '{}' df range [{}, {}]".format(name, start_date, end_date))

    df = df.withColumn(
        'IMPRESSION', 
        df['DOWNLOAD'] +  df['LIKED'] * 4 +  df['PURCHASE'] * 2
    )
    df.show()
    print("JOB -> add impression to '{}' df".format(name))
    
    df = df.join(minimal_content_based, ["TRACK_ID"], "inner")
    print("JOB -> joined '{}' df with minimal content based".format(name))
#     df.show()
    print("JOB -> showed joined df")

    df.printSchema()
#     df.write.csv(data_path + '{}-actions-v2_{}_{}'.format(name, start_date, end_date), header=False)
    print("JOB -> save '{}' df".format(name))

    return df

def read_aggregated_actions(start_date, end_date, name):
    aggregated_schema = StructType([
        StructField("TRACK_ID", StringType(), False),
        StructField("USER_ID", IntegerType(), False),
        StructField("LIKED", FloatType(), False),
        StructField("DOWNLOAD", FloatType(), False),
        StructField("PURCHASE", FloatType(), False),
        StructField("IMPRESSION", FloatType(), False)
    ])
    file_name = data_path + '{}-actions_{}_{}/data.csv'.format(name, start_date, end_date)
    print("JOB -> start reading '{}' file".format(file_name))
    
    df = spark.read.csv(file_name, inferSchema=True, header=True, sep=",")
    df.printSchema()
    df.show()
    print("JOB -> read '{}' aggregatd action csv of [{},{}]".format(name, start_date, end_date))
    return df

def learn_model(train_actions, test_actions, implicit=False):
    als = ALS(maxIter=10, regParam=0.15, rank=100, 
            userCol="USER_ID", itemCol="TRACK_ID", ratingCol="IMPRESSION",
            coldStartStrategy="drop",
            implicitPrefs=implicit)
    model = als.fit(train_actions)
    print("JOB -> implicit={} model -> traind".format(implicit))

    # Evaluate the model by computing the metric on the test data
    predictions = model.transform(test_actions)
    print("JOB -> implicit={} model -> test data transformed".format(implicit))

    evaluator = RegressionEvaluator(metricName="mae", labelCol="IMPRESSION", predictionCol="prediction")
    metric = evaluator.evaluate(predictions)
    print("JOB -> implicit={} model -> mean absolute error={}".format(implicit, metric))
    return model, metric

def array_to_string(data):
    return '[' + ','.join([str(elem[0]) for elem in data]) + ']'

def tune_ALS(train_data, validation_data, maxIter, regParams, ranks, implicit=False):
    """
    grid search function to select the best model based on RMSE of
    validation data
    Parameters
    ----------
    train_data: spark DF with columns ['userId', 'movieId', 'rating']
    
    validation_data: spark DF with columns ['userId', 'movieId', 'rating']
    
    maxIter: int, max number of learning iterations
    
    regParams: list of float, one dimension of hyper-param tuning grid
    
    ranks: list of float, one dimension of hyper-param tuning grid
    
    Return
    ------
    The best fitted ALS model with lowest RMSE score on validation data
    """
    # initial
    min_error = float('inf')
    best_rank = -1
    best_regularization = 0
    best_model = None
    for rank in ranks:
        for reg in regParams:
            # get ALS model
            # als = ALS().setMaxIter(maxIter).setRank(rank).setRegParam(reg)
            als = ALS(maxIter=maxIter, regParam=reg, rank=rank,
            userCol="USER_ID", itemCol="TRACK_ID", ratingCol="IMPRESSION",
            coldStartStrategy="drop",
            implicitPrefs=implicit)
            # train ALS model
            model = als.fit(train_data)
            # evaluate the model by computing the RMSE on the validation data
            predictions = model.transform(validation_data)
            evaluator = RegressionEvaluator(metricName="mae",
                                            labelCol="IMPRESSION",
                                            predictionCol="prediction")
            rmse = evaluator.evaluate(predictions)
            print('JOB -> {} latent factors and regularization = {}: '
                  'validation MAE is {}'.format(rank, reg, rmse))
            if rmse < min_error:
                min_error = rmse
                best_rank = rank
                best_regularization = reg
                best_model = model
    print('\nJOB -> The best model has {} latent factors and '
          'regularization = {}'.format(best_rank, best_regularization))
    return best_model

def recommend(model):
    array_to_string_udf = udf(array_to_string, StringType())

    # Generate top 10 track recommendations for each user
    user_recs = model.recommendForAllUsers(10)
    print("JOB -> user recommendation list is ready")
    # print("JOB -> num of recommended user: {}".format(user_recs.count()))
    # user_recs.show()
    print("JOB -> converting user recommedation array column")
    user_recs = user_recs.withColumn('RECOMMENDATIONS_STR', array_to_string_udf(user_recs["recommendations"])).drop("recommendations")
    print("JOB -> user recommedation array column converted to string")
    user_recs.write.csv(data_path + 'user_recs_v3_{}_{}'.format(test_start_date, test_end_date), header=False)
    print("JOB -> user recommendations saved")


    # Generate top 10 user recommendations for each track
    track_recs = model.recommendForAllItems(10)
    print("JOB -> track recommendation list is ready")
    # print("JOB -> num of recommended tracks: {}".format(track_recs.count()))
    # track_recs.show()
    print("JOB -> converting track recommedation array column")
    track_recs = track_recs.withColumn('RECOMMENDATIONS_STR', array_to_string_udf(track_recs["recommendations"])).drop("recommendations")
    print("JOB -> track recommedation array column converted to string")
    track_recs.write.csv(data_path + 'track_recs_v3_{}_{}'.format(test_start_date, test_end_date), header=False)
    print("JOB -> track recommendations saved")

if __name__ == "__main__":
    spark = start_spark()
    
    if mode == "FILE":
        train_actions = read_aggregated_actions(train_start_date, train_end_date, name='train')
        test_actions = read_aggregated_actions(test_start_date, test_end_date, name='test')
    elif mode == "COMPUTE":
        actions = read_actions_csv(spark)
        minimal_content_based = read_minimal_content_based_csv(spark)
        train_actions = aggregate_actions(actions, minimal_content_based, train_start_date, train_end_date, name='train')
        test_actions = aggregate_actions(actions, minimal_content_based, test_start_date, test_end_date, name='test')
    
    implicit_model, implicit_error = learn_model(train_actions, test_actions, implicit=True)
    explicit_model, explicit_error = learn_model(train_actions, test_actions, implicit=False)
    #tune_model = tune_ALS(train_actions,test_actions,10,[0.1, 0.15, 0.2, 0.25],[50, 100, 150, 200],implicit=False)

#     model = implicit_model if implicit_error <= explicit_error else explicit_model
#     recommend(model)

    spark.stop()
