PID = '' # your pid, for instance: 'a43223333'
INPUT_FORMAT = 'dataframe' # choose a format of your input data, valid options: 'dataframe', 'rdd', 'koalas'

# Boiler plates, do NOT modify
import os
import getpass
from pyspark.sql import SparkSession
from utilities import SEED
from utilities import PA2Test
from utilities import PA2Data
from utilities import data_cat
from pa2_main import PA2Executor
import time

import pyspark.sql.functions as F

if INPUT_FORMAT == 'dataframe':
    import pyspark.ml as M
    import pyspark.sql.functions as F
    import pyspark.sql.types as T
if INPUT_FORMAT == 'koalas':
    import databricks.koalas as ks
elif INPUT_FORMAT == 'rdd':
    import pyspark.mllib as M
    from pyspark.mllib.feature import Word2Vec
    from pyspark.mllib.linalg import Vectors
    from pyspark.mllib.linalg.distributed import RowMatrix

os.environ['PYSPARK_SUBMIT_ARGS'] = '--py-files utilities.py,assignment2.py \
--deploy-mode client \
pyspark-shell'

class args:
    review_filename = data_cat.review_filename
    product_filename = data_cat.product_filename
    product_processed_filename = data_cat.product_processed_filename
    ml_features_train_filename = data_cat.ml_features_train_filename
    ml_features_test_filename = data_cat.ml_features_test_filename
    output_root = '/home/{}/{}-pa2/test_results'.format(getpass.getuser(), PID)
    test_results_root = data_cat.test_results_root
    pid = PID

pa2 = PA2Executor(args, input_format=INPUT_FORMAT)
data_io = pa2.data_io
data_dict = pa2.data_dict
begin = time.time()

# %load -s task_1 assignment2.py
def task_1(data_io, review_data, product_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    asin_column = 'asin'
    overall_column = 'overall'
    # Outputs:
    mean_rating_column = 'meanRating'
    count_rating_column = 'countRating'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------
    
    review = review_data
    product = product_data
    
    rating_stats = review.groupBy("asin").agg(
        F.mean("overall").alias("meanRating"),
        F.count("overall").alias("countRating")
    )
    joined = product.select("asin").join(rating_stats, on="asin", how="left")


    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    # Calculate the values programmaticly. Do not change the keys and do not
    # hard-code values in the dict. Your submission will be evaluated with
    # different inputs.
    # Modify the values of the following dictionary accordingly.
    res = {
        'count_total': joined.count(),
        'mean_meanRating': float(joined.select(F.mean("meanRating")).first()[0]),
        'variance_meanRating': float(joined.select(F.variance("meanRating")).first()[0]),
        'numNulls_meanRating': joined.filter(F.col("meanRating").isNull()).count(),
        'mean_countRating': float(joined.select(F.mean("countRating")).first()[0]),
        'variance_countRating': float(joined.select(F.variance("countRating")).first()[0]),
        'numNulls_countRating': joined.filter(F.col("countRating").isNull()).count()
    }
    # Modify res:




    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_1')
    return res
    # -------------------------------------------------------------------------

def task_2(data_io, product_data):
    # -----------------------------Column names--------------------------------
    salesRank_column = 'salesRank'
    categories_column = 'categories'
    asin_column = 'asin'

    category_column = 'category'
    bestSalesCategory_column = 'bestSalesCategory'
    bestSalesRank_column = 'bestSalesRank'
    # -------------------------------------------------------------------------

    import pyspark.sql.functions as F

    # ---------------------- Your implementation begins------------------------

    # Extract top-level category with proper null/empty checking
    df = product_data.withColumn(
        category_column,
        F.when(
            (F.col(categories_column).isNotNull()) &
            (F.size(F.col(categories_column)) > 0) &
            (F.size(F.col(categories_column)[0]) > 0),
            F.col(categories_column)[0][0]
        ).otherwise(None)
    )

    # Replace empty string category with null (fixes overcounting distinct)
    df = df.withColumn(
        category_column,
        F.when(F.col(category_column) == "", None).otherwise(F.col(category_column))
    )

    # Extract bestSalesCategory and bestSalesRank from the map
    df = df.withColumn(bestSalesCategory_column, F.when(
        F.col(salesRank_column).isNotNull(),
        F.map_keys(F.col(salesRank_column))[0]
    ).otherwise(None))

    df = df.withColumn(bestSalesRank_column, F.when(
        F.col(salesRank_column).isNotNull(),
        F.map_values(F.col(salesRank_column))[0]
    ).otherwise(None))

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': df.count(),
        'mean_bestSalesRank': float(df.select(F.mean(bestSalesRank_column)).first()[0]),
        'variance_bestSalesRank': float(df.select(F.variance(bestSalesRank_column)).first()[0]),
        'numNulls_category': df.filter(F.col(category_column).isNull()).count(),
        'countDistinct_category': df.select(category_column).distinct().filter(F.col(category_column).isNotNull()).count(),
        'numNulls_bestSalesCategory': df.filter(F.col(bestSalesCategory_column).isNull()).count(),
        'countDistinct_bestSalesCategory': df.select(bestSalesCategory_column).distinct().filter(F.col(bestSalesCategory_column).isNotNull()).count()
    }

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_2')
    return res

    def task_3(data_io, product_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    asin_column = 'asin'
    price_column = 'price'
    attribute = 'also_viewed'
    related_column = 'related'
    # Outputs:
    meanPriceAlsoViewed_column = 'meanPriceAlsoViewed'
    countAlsoViewed_column = 'countAlsoViewed'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------
    price_lookup = product_data.select(asin_column, price_column).filter(F.col(price_column).isNotNull())

    product_data = product_data.withColumn(
        attribute,
        F.when(
            (F.col(related_column).isNotNull()) & (F.col(related_column)[attribute].isNotNull()),
            F.col(related_column)[attribute]
        ).otherwise(None)
    )

    product_data = product_data.withColumn(
        countAlsoViewed_column,
        F.when(
            F.col(attribute).isNotNull() & (F.size(F.col(attribute)) > 0),
            F.size(F.col(attribute))
        ).otherwise(None)
    )

    exploded = product_data.select(asin_column, attribute).withColumn(
        "related_asin",
        F.explode_outer(attribute)
    )

    exploded_with_price = exploded.join(
        price_lookup,
        exploded["related_asin"] == price_lookup[asin_column],
        how="left"
    ).select(exploded[asin_column], price_column)

    mean_prices = exploded_with_price.groupBy(asin_column).agg(
        F.mean(price_column).alias(meanPriceAlsoViewed_column)
    )

    result = product_data.select(asin_column, countAlsoViewed_column).join(
        mean_prices, on=asin_column, how="left"
    )

    count_total = result.count()

    mean_meanPriceAlsoViewed = result.select(F.mean(meanPriceAlsoViewed_column)).first()[0]
    variance_meanPriceAlsoViewed = result.select(F.variance(meanPriceAlsoViewed_column)).first()[0]
    numNulls_meanPriceAlsoViewed = result.filter(F.col(meanPriceAlsoViewed_column).isNull()).count()

    mean_countAlsoViewed = result.select(F.mean(countAlsoViewed_column)).first()[0]
    variance_countAlsoViewed = result.select(F.variance(countAlsoViewed_column)).first()[0]
    numNulls_countAlsoViewed = result.filter(F.col(countAlsoViewed_column).isNull()).count()




    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': None,
        'mean_meanPriceAlsoViewed': None,
        'variance_meanPriceAlsoViewed': None,
        'numNulls_meanPriceAlsoViewed': None,
        'mean_countAlsoViewed': None,
        'variance_countAlsoViewed': None,
        'numNulls_countAlsoViewed': None
    }
    # Modify res:
    res = {
        'count_total': int(count_total),
        'mean_meanPriceAlsoViewed': float(mean_meanPriceAlsoViewed) if mean_meanPriceAlsoViewed is not None else None,
        'variance_meanPriceAlsoViewed': float(variance_meanPriceAlsoViewed) if variance_meanPriceAlsoViewed is not None else None,
        'numNulls_meanPriceAlsoViewed': int(numNulls_meanPriceAlsoViewed),
        'mean_countAlsoViewed': float(mean_countAlsoViewed) if mean_countAlsoViewed is not None else None,
        'variance_countAlsoViewed': float(variance_countAlsoViewed) if variance_countAlsoViewed is not None else None,
        'numNulls_countAlsoViewed': int(numNulls_countAlsoViewed)
    }

    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_3')
    return 

    def task_4(data_io, product_data):
    # -----------------------------Column names--------------------------------
    price_column = 'price'
    title_column = 'title'

    meanImputedPrice_column = 'meanImputedPrice'
    medianImputedPrice_column = 'medianImputedPrice'
    unknownImputedTitle_column = 'unknownImputedTitle'
    # -------------------------------------------------------------------------

    import pyspark.sql.functions as F

    # ---------------------- Your implementation begins------------------------

    # Cast price to float (in case it's not)
    df = product_data.withColumn(price_column, F.col(price_column).cast("float"))

    # Compute mean and median of non-null prices
    mean_price = df.select(F.mean(price_column)).first()[0]
    median_price = df.approxQuantile(price_column, [0.5], 0.01)[0]

    # Impute mean
    df = df.withColumn(meanImputedPrice_column,
        F.when(F.col(price_column).isNull(), mean_price).otherwise(F.col(price_column))
    )

    # Impute median
    df = df.withColumn(medianImputedPrice_column,
        F.when(F.col(price_column).isNull(), median_price).otherwise(F.col(price_column))
    )

    # Impute unknown titles
    df = df.withColumn(unknownImputedTitle_column,
        F.when((F.col(title_column).isNull()) | (F.col(title_column) == ""), "unknown")
         .otherwise(F.col(title_column))
    )

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': df.count(),
        'mean_meanImputedPrice': float(df.select(F.mean(meanImputedPrice_column)).first()[0]),
        'variance_meanImputedPrice': float(df.select(F.variance(meanImputedPrice_column)).first()[0]),
        'numNulls_meanImputedPrice': df.filter(F.col(meanImputedPrice_column).isNull()).count(),
        'mean_medianImputedPrice': float(df.select(F.mean(medianImputedPrice_column)).first()[0]),
        'variance_medianImputedPrice': float(df.select(F.variance(medianImputedPrice_column)).first()[0]),
        'numNulls_medianImputedPrice': df.filter(F.col(medianImputedPrice_column).isNull()).count(),
        'numUnknowns_unknownImputedTitle': df.filter(F.col(unknownImputedTitle_column) == "unknown").count()
    }

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_4')
    return res
    # -------------------------------------------------------------------------


def task_5(data_io, product_processed_data, word_0, word_1, word_2):
    # -----------------------------Column names--------------------------------
    title_column = 'title'
    titleArray_column = 'titleArray'
    # -------------------------------------------------------------------------

    import pyspark.sql.functions as F
    from pyspark.ml.feature import Word2Vec

    # ---------------------- Your implementation begins------------------------

    # Step 1: Convert title to lowercase and split by whitespace
    product_processed_data_output = product_processed_data.withColumn(
        titleArray_column,
        F.split(F.lower(F.col(title_column)), " ")
    )

    # Step 2: Train Word2Vec model
    word2vec = Word2Vec(
        inputCol=titleArray_column,
        outputCol="titleVector",
        vectorSize=16,
        minCount=100,
        seed=102,
        numPartitions=4
    )

    model = word2vec.fit(product_processed_data_output)

    # Step 3: Find synonyms for word_0, word_1, word_2
    def get_synonyms(word):
        return [(row["word"], float(row["similarity"])) for row in model.findSynonyms(word, 10).collect()]

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': product_processed_data_output.count(),
        'size_vocabulary': model.getVectors().count(),
        'word_0_synonyms': get_synonyms(word_0),
        'word_1_synonyms': get_synonyms(word_1),
        'word_2_synonyms': get_synonyms(word_2)
    }

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_5')
    return res

def task_6(data_io, product_processed_data):
    # -----------------------------Column names--------------------------------
    category_column = 'category'
    categoryIndex_column = 'categoryIndex'
    categoryOneHot_column = 'categoryOneHot'
    categoryPCA_column = 'categoryPCA'
    # -------------------------------------------------------------------------

    import pyspark.sql.functions as F
    from pyspark.ml.feature import StringIndexer, OneHotEncoder, PCA
    from pyspark.ml.linalg import DenseVector
    from pyspark.ml.stat import Summarizer

    # Step 1: StringIndexing
    indexer = StringIndexer(inputCol=category_column, outputCol=categoryIndex_column, handleInvalid="skip")
    df_indexed = indexer.fit(product_processed_data).transform(product_processed_data)

    # Step 2: One-Hot Encoding
    encoder = OneHotEncoder(
        inputCols=[categoryIndex_column],
        outputCols=[categoryOneHot_column],
        dropLast=False
    )
    df_encoded = encoder.fit(df_indexed).transform(df_indexed)

    # Step 3: PCA
    pca = PCA(k=15, inputCol=categoryOneHot_column, outputCol=categoryPCA_column)
    pca_model = pca.fit(df_encoded)
    df_pca = pca_model.transform(df_encoded)

    # Step 4: Compute mean vectors using Summarizer
    summarizer = Summarizer.metrics("mean")

    meanVector_categoryOneHot = df_pca.select(Summarizer.mean(F.col(categoryOneHot_column))).first()[0].toArray().tolist()
    meanVector_categoryPCA = df_pca.select(Summarizer.mean(F.col(categoryPCA_column))).first()[0].toArray().tolist()

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': df_pca.count(),
        'meanVector_categoryOneHot': meanVector_categoryOneHot,
        'meanVector_categoryPCA': meanVector_categoryPCA
    }

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_6')
    return res

    def task_7(data_io, train_data, test_data):
    # ---------------------- Your implementation begins------------------------
    from pyspark.ml.regression import DecisionTreeRegressor
    from pyspark.ml.evaluation import RegressionEvaluator

    # Step 1: Initialize and fit the model
    model = DecisionTreeRegressor(
        featuresCol="features",
        labelCol="overall",
        maxDepth=5
    )

    model_fitted = model.fit(train_data)

    # Step 2: Generate predictions on test data
    predictions = model_fitted.transform(test_data)

    # Step 3: Evaluate RMSE
    evaluator = RegressionEvaluator(
        labelCol="overall",
        predictionCol="prediction",
        metricName="rmse"
    )

    test_rmse = evaluator.evaluate(predictions)

    # ---------------------- Put results in res dict --------------------------
    res = {
        'test_rmse': float(test_rmse)
    }

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_7')
    return res

def task_8(data_io, train_data, test_data):
    # ---------------------- Your implementation begins------------------------
    from pyspark.ml.regression import DecisionTreeRegressor
    from pyspark.ml.evaluation import RegressionEvaluator

    # Step 1: Split into train and validation sets
    train_split, val_split = train_data.randomSplit([0.75, 0.25], seed=102)

    # Step 2: Prepare evaluator
    evaluator = RegressionEvaluator(
        labelCol="overall",
        predictionCol="prediction",
        metricName="rmse"
    )

    # Step 3: Train and evaluate for different depths
    depths = [5, 7, 9, 12]
    val_rmses = {}

    for depth in depths:
        model = DecisionTreeRegressor(
            featuresCol="features",
            labelCol="overall",
            maxDepth=depth
        ).fit(train_split)

        val_preds = model.transform(val_split)
        val_rmse = evaluator.evaluate(val_preds)
        val_rmses[depth] = val_rmse

    # Step 4: Select best depth based on validation RMSE
    best_depth = min(val_rmses, key=val_rmses.get)

    # Step 5: Train best model on full train_data and evaluate on test_data
    best_model = DecisionTreeRegressor(
        featuresCol="features",
        labelCol="overall",
        maxDepth=best_depth
    ).fit(train_data)

    test_preds = best_model.transform(test_data)
    test_rmse = evaluator.evaluate(test_preds)

    # ---------------------- Put results in res dict --------------------------
    res = {
        'test_rmse': float(test_rmse),
        'valid_rmse_depth_5': float(val_rmses[5]),
        'valid_rmse_depth_7': float(val_rmses[7]),
        'valid_rmse_depth_9': float(val_rmses[9]),
        'valid_rmse_depth_12': float(val_rmses[12])
    }

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_8')
    return res
