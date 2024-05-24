from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, when, udf
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, Imputer
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression, GBTClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import requests
from bs4 import BeautifulSoup
import pandas as pd

NUMBER_OF_FOLDS = 5
TRAIN_TEST_SPLIT = 0.9
SPLIT_SEED = 42

url1 = 'https://www.abs.gov.au/statistics/health/health-conditions-and-risks/smoking/latest-release'
url2 = 'https://www.cdc.gov/tobacco/data_statistics/fact_sheets/adult_data/cig_smoking/index.htm'

def read_data(spark: SparkSession) -> DataFrame:
    """
    Read data; since the data has the header we let spark guess the schema
    """
    heartdisease = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("s3://de300spring2024/cindy_vong/heart_disease(in).csv")

    heartdisease = heartdisease.select('age', 'sex', 'painloc', 'painexer', 'cp', 'trestbps', 'smoke', 'fbs', 'prop', 
                                       'nitr', 'pro', 'diuretic', 'thaldur', 'thalach', 'exang', 'oldpeak', 'slope', 
                                       'target')
    return heartdisease

def extract_tables(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    tables = soup.find_all('table')
    html = str(tables[2])
    soup = BeautifulSoup(html, 'html.parser')
    
    rows = soup.find_all('tr')
    column_headers = [th.text.strip() for th in rows[0].find_all('th') if th.text.strip()]

    data = []
    for row in rows[1:]:
        row_header = row.find('th').text.strip()
        row_data = [row_header] + [td.text.strip() for td in row.find_all('td')]
        data.append(row_data)

    df = pd.DataFrame(data, columns=['Age Group'] + column_headers)
    return df

def extract_cdc(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    list_items = soup.find_all('li')
    mr, wr = None, None
    for item in list_items:
        text = item.get_text()
        if "adult men" in text:
            mr = float(text.split('(')[-1].split('%')[0])
        elif "adult women" in text:
            wr = float(text.split('(')[-1].split('%')[0])

    age_groups_rates = {}
    age_groups_rates['female'] = wr / 100
    age_groups_rates['male'] = mr / 100

    age_group_tags = soup.find_all('h4', class_='card-header')
    for tag in age_group_tags:
        if 'By Age' in tag.text:
            age_group_data = tag.find_next('div', class_='card-body').find_all('li')
            for data in age_group_data:
                age_group_text = data.get_text()
                if 'every' in age_group_text:
                    age_group = age_group_text.split(' ')[-3]
                    rate = float(age_group_text.split('(')[-1].split('%')[0])
                    age_groups_rates[age_group] = float(rate) / 100
    return age_groups_rates

def get_smoking_rate1(age, sex, smoke):
    if smoke is not None:
        return smoke
    if age < 17:
        age_group = '15–17(a)'
    elif age < 25:
        age_group = '18–24'
    elif age < 35:
        age_group = '25–34'
    elif age < 45:
        age_group = '35–44'
    elif age < 55:
        age_group = '45–54'
    elif age < 65:
        age_group = '55–64'
    elif age < 75:
        age_group = '65–74'
    else:
        age_group = '75 years and over'

    # Extract smoking rates from the provided URLs
    smokingrates1 = extract_tables(url1)

    # Retrieve smoking rate based on age group and gender
    if pd.notna(smoke):
        return smoke
    elif sex == 1:
        return float(smokingrates1.loc[smokingrates1['Age Group'] == age_group, 'Males (%)'].values[0])/100
    elif sex == 0:
        return float(smokingrates1.loc[smokingrates1['Age Group'] == age_group, 'Females (%)'].values[0])/100
    else:
        return None

def get_smoking_rate2(age, sex, smoke):
    if smoke is not None:
        return smoke
    if age <= 24:
        group = "18–24"
    elif age <= 44:
        group = "25–44"
    elif age <= 64:
        group = "45–64"
    else:
        group = "and"
    
    # Extract smoking rates from the provided URLs
    smokingrates2 = extract_cdc(url2)

    female_rate = smokingrates2.get('female')
    male_rate = smokingrates2.get('male')
    group_rate = smokingrates2.get(group)

    if sex == 0:
        return group_rate
    else:
        return group_rate * male_rate / female_rate

def pipeline(data: DataFrame):
    # UDFs for smoking rates
    get_smoking_rate1_udf = udf(get_smoking_rate1, DoubleType())
    get_smoking_rate2_udf = udf(get_smoking_rate2, DoubleType())
    
    # Apply UDFs to create smoke1 and smoke2 columns
    data = data.withColumn('smoke1', get_smoking_rate1_udf(col('age'), col('sex'), col('smoke')))
    data = data.withColumn('smoke2', get_smoking_rate2_udf(col('age'), col('sex'), col('smoke')))

    # Clean and impute steps for specific columns
    data = data.withColumn("trestbps", when(col("trestbps") < 100, 100).otherwise(col("trestbps")))
    data = data.withColumn("oldpeak", when(col("oldpeak") < 0, 0).when(col("oldpeak") > 4, 4).otherwise(col("oldpeak")))

    imputer_cols = ["painloc", "painexer", "thaldur", "thalach", "fbs", "prop", "nitr", "pro", "diuretic", "exang", "slope", "smoke1", "smoke2"]
    imputer = Imputer(inputCols=imputer_cols, outputCols=[f"{col}_imputed" for col in imputer_cols])

    for col_name in ["fbs", "prop", "nitr", "pro", "diuretic"]:
        data = data.withColumn(col_name, when(col(col_name) > 1, 1).otherwise(col(col_name)))

    # Assemble feature columns into a single feature vector
    assembler = VectorAssembler(
        inputCols=[f"{col}_imputed" if col in imputer_cols else col for col in data.columns if col != "target"],
        outputCol="features"
    )

    # Define classifiers
    rf_classifier = RandomForestClassifier(labelCol="target", featuresCol="features")
    lr_classifier = LogisticRegression(labelCol="target", featuresCol="features")
    gbt_classifier = GBTClassifier(labelCol="target", featuresCol="features")

    # Create pipelines for each classifier
    rf_pipeline = Pipeline(stages=[imputer, assembler, rf_classifier])
    lr_pipeline = Pipeline(stages=[imputer, assembler, lr_classifier])
    gbt_pipeline = Pipeline(stages=[imputer, assembler, gbt_classifier])
    
    # Set up the parameter grids
    rf_paramGrid = ParamGridBuilder() \
        .addGrid(rf_classifier.maxDepth, [2, 4, 6, 8, 10]) \
        .addGrid(rf_classifier.numTrees, [50, 100, 150]) \
        .build()
    lr_paramGrid = ParamGridBuilder() \
        .addGrid(lr_classifier.regParam, [0.01, 0.1, 1.0]) \
        .addGrid(lr_classifier.elasticNetParam, [0.0, 0.5, 1.0]) \
        .build()
    gbt_paramGrid = ParamGridBuilder() \
        .addGrid(gbt_classifier.maxDepth, [2, 4, 6, 8, 10]) \
        .addGrid(gbt_classifier.maxIter, [10, 20, 30]) \
        .build()

    # Set up the cross-validators
    evaluator = BinaryClassificationEvaluator(labelCol="target", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    rf_crossval = CrossValidator(estimator=rf_pipeline, estimatorParamMaps=rf_paramGrid, evaluator=evaluator, numFolds=NUMBER_OF_FOLDS)
    lr_crossval = CrossValidator(estimator=lr_pipeline, estimatorParamMaps=lr_paramGrid, evaluator=evaluator, numFolds=NUMBER_OF_FOLDS)
    gbt_crossval = CrossValidator(estimator=gbt_pipeline, estimatorParamMaps=gbt_paramGrid, evaluator=evaluator, numFolds=NUMBER_OF_FOLDS)

    # Split the data into training and test sets
    train_data, test_data = data.randomSplit([TRAIN_TEST_SPLIT, 1-TRAIN_TEST_SPLIT], seed=SPLIT_SEED)

    # Train the cross-validated models
    rf_cvModel = rf_crossval.fit(train_data)
    lr_cvModel = lr_crossval.fit(train_data)
    gbt_cvModel = gbt_crossval.fit(train_data)

    # Make predictions on the test data
    rf_predictions = rf_cvModel.transform(test_data)
    lr_predictions = lr_cvModel.transform(test_data)
    gbt_predictions = gbt_cvModel.transform(test_data)

    # Evaluate the models
    rf_auc = evaluator.evaluate(rf_predictions)
    lr_auc = evaluator.evaluate(lr_predictions)
    gbt_auc = evaluator.evaluate(gbt_predictions)

    # Print the performance metrics
    print(f"Random Forest - Area Under ROC Curve: {rf_auc:.4f}")
    print(f"Logistic Regression - Area Under ROC Curve: {lr_auc:.4f}")
    print(f"Gradient Boosting - Area Under ROC Curve: {gbt_auc:.4f}")

    # Get the best models
    rf_best_model = rf_cvModel.bestModel.stages[-1]
    lr_best_model = lr_cvModel.bestModel.stages[-1]
    gbt_best_model = gbt_cvModel.bestModel.stages[-1]

    # Retrieve the selected hyperparameters
    rf_selected_max_depth = rf_best_model.getOrDefault(rf_best_model.getParam("maxDepth"))
    rf_selected_num_trees = rf_best_model.getOrDefault(rf_best_model.getParam("numTrees"))
    lr_selected_reg_param = lr_best_model.getOrDefault(lr_best_model.getParam("regParam"))
    lr_selected_elastic_net_param = lr_best_model.getOrDefault(lr_best_model.getParam("elasticNetParam"))
    gbt_selected_max_depth = gbt_best_model.getOrDefault(gbt_best_model.getParam("maxDepth"))
    gbt_selected_max_iter = gbt_best_model.getOrDefault(gbt_best_model.getParam("maxIter"))

    # Print the selected hyperparameters
    print(f"Random Forest - Selected Maximum Tree Depth: {rf_selected_max_depth}")
    print(f"Random Forest - Selected Number of Trees: {rf_selected_num_trees}")
    print(f"Logistic Regression - Selected Regularization Parameter: {lr_selected_reg_param}")
    print(f"Logistic Regression - Selected Elastic Net Parameter: {lr_selected_elastic_net_param}")
    print(f"Gradient Boosting - Selected Maximum Tree Depth: {gbt_selected_max_depth}")
    print(f"Gradient Boosting - Selected Maximum Iterations: {gbt_selected_max_iter}")

def main():
    # Create a Spark session
    spark = SparkSession.builder \
        .appName("Predict Heart Disease") \
        .getOrCreate()

    # Read data and apply pipeline
    data = read_data(spark)
    pipeline(data)

    spark.stop()

main()
