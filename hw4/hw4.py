from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, when
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, Imputer
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression, GBTClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import boto3
import pandas as pd
from io import StringIO
import tomli
import pathlib
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean as _mean, stddev as _stddev, abs as _abs
import requests
from scrapy import Selector
from bs4 import BeautifulSoup
from sklearn.metrics import accuracy_score

# read the parameters from toml
CONFIG_FILE = "s3://de300-airflow-cindy//config2.toml"

TABLE_NAMES = {
    "original_data": "heart_disease",
    "clean_data_spark": "spark_heart_disease_clean",
    "clean_data_pandas" : "pandas_heart_disease_clean",
    "train_data": "heart_disease_train",
    "test_data": "heart_disease_test",
    "normalization_data": "normalization_values",
    "merged_data": "heart_disease_merged",
    "spark_fe": "max_fe_features",
    "pandas_fe": "product_fe_features"
}

ENCODED_SUFFIX = "_encoded"

# Define the default args dictionary for DAG
default_args = {
    'owner': 'cindyvong',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

def read_config_from_s3() -> dict:
    # Parse the bucket name and file key from the S3 path
    bucket_name = CONFIG_FILE.split('/')[2]
    key = '/'.join(CONFIG_FILE.split('/')[3:])[1:]

    # Create a boto3 S3 client
    s3_client = boto3.client('s3')

    try:
        # Fetch the file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        file_content = response['Body'].read()

        # Load the TOML content
        params = tomli.loads(file_content.decode('utf-8'))
        return params
    except Exception as e:
        print(f"Failed to read from S3: {str(e)}")
        return {}

# Usage
PARAMS = read_config_from_s3()


def create_db_connection():
    conn_uri = f"{PARAMS['db']['db_alchemy_driver']}://{PARAMS['db']['username']}:{PARAMS['db']['password']}@{PARAMS['db']['host']}:{PARAMS['db']['port']}/{PARAMS['db']['db_name']}"

    # Create a SQLAlchemy engine and connect
    engine = create_engine(conn_uri)
    connection = engine.connect()

    return connection

def from_table_to_df(input_table_names: list[str], output_table_names: list[str]):
    def decorator(func):
        def wrapper(*args, **kwargs):
            import pandas as pd

            if input_table_names is None:
                raise ValueError('input_table_names cannot be None')
            
            _input_table_names = None
            if isinstance(input_table_names, str):
                _input_table_names = [input_table_names]
            else:
                _input_table_names = input_table_names

            print(f'Loading input tables to dataframes: {_input_table_names}')

            # open the connection
            conn = create_db_connection()

            # read tables and convert to dataframes
            dfs = []
            for table_name in _input_table_names:
                df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
                dfs.append(df)

            if isinstance(input_table_names, str):
                dfs = dfs[0]

            kwargs['dfs'] = dfs
            kwargs['output_table_names'] = output_table_names
            result = func(*args, **kwargs)

            print(f'Deleting tables: {output_table_names}')
            if output_table_names is None:
                _output_table_names = []
            elif isinstance(output_table_names, str):
                _output_table_names = [output_table_names]
            else:
                _output_table_names = output_table_names
            
            print(f"Dropping tables {_output_table_names}")
            for table_name in _output_table_names:
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            for pairs in result['dfs']:
                df = pairs['df']
                table_name = pairs['table_name']
                df.to_sql(table_name, conn, if_exists="replace", index=False)
                print(f"Wrote to table {table_name}")

            conn.close()
            result.pop('dfs')

            return result
        return wrapper
    return decorator

def add_data_to_table_func(**kwargs):
    """
    Insert data from a CSV file stored in S3 to a database table.
    """
    # Create a database connection
    conn = create_db_connection()
    
    # Set the S3 bucket and file key
    s3_bucket = PARAMS['files']['s3_bucket']
    s3_key = PARAMS['files']['s3_file_key']
    
    # Create an S3 client
    s3_client = boto3.client('s3')
    
    # Get the object from the S3 bucket
    response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
    
    # Read the CSV file directly from the S3 object's byte stream into a DataFrame
    csv_content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(csv_content))
    
    # Write the DataFrame to SQL
    df.to_sql(TABLE_NAMES['original_data'], con=conn, if_exists="replace", index=False)

    # Close the database connection
    conn.close()

    return {'status': 1}


def create_spark_session():
    spark = SparkSession.builder \
        .appName("Airflow Spark Pipeline") \
        .getOrCreate()
    return spark

def extract_tables():
    url = 'https://www.abs.gov.au/statistics/health/health-conditions-and-risks/smoking/latest-release'
    response = requests.get(url)
    html_content = response.content

    full_sel = Selector(text=html_content)
    tables = full_sel.xpath('//table')
    html = tables[2].extract()

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


# Function to extract CDC data from a given URL
def extract_cdc():
    url = 'https://www.cdc.gov/tobacco/data_statistics/fact_sheets/adult_data/cig_smoking/index.htm'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    list_items = soup.find_all('li')
    age_groups_rates = {}

    for item in list_items:
        text = item.get_text()
        if "adult men" in text:
            mr = float(text.split('(')[-1].split('%')[0])
        elif "adult women" in text:
            wr = float(text.split('(')[-1].split('%')[0])

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

# Function to scrape data from the web
def scrape_data_func(**kwargs):
    smokingrates1 = extract_tables()
    smokingrates2 = extract_cdc()
    
    # Rename columns in smokingrates1
    smokingrates1 = rename_columns(smokingrates1, suffix='_1')
    
    # Rename columns in smokingrates2
    smokingrates2 = rename_columns(smokingrates2, suffix='_2')
    
    kwargs['ti'].xcom_push(key='smokingrates1', value=smokingrates1)
    kwargs['ti'].xcom_push(key='smokingrates2', value=smokingrates2)

def rename_columns(data, suffix):
    # Add a suffix to each column name
    renamed_columns = [col + suffix for col in data.columns]
    data.columns = renamed_columns
    return data

# Function to merge scraped data with feature-engineered datasets
@from_table_to_df([TABLE_NAMES['spark_fe'], TABLE_NAMES['pandas_fe']], TABLE_NAMES["merged_data"])
def merge_scraped_data_func(**kwargs):
    # Get web scraped data
    smoking_rates1 = kwargs['ti'].xcom_pull(key='smokingrates1', task_ids='scrape_data')
    smoking_rates2 = kwargs['ti'].xcom_pull(key='smokingrates2', task_ids='scrape_data')

    CONN_URI=f"{PARAMS['db']['db_alchemy_driver']}://{PARAMS['db']['username']}:{PARAMS['db']['password']}@{PARAMS['db']['host']}:{PARAMS['db']['port']}/{PARAMS['db']['db_name']}"

    def get_smoking_rate1(row, rates_df):
        age = row['age']
        sex = row['sex']
        age_groups = {
            1: '15–17(a)',
            2: '18–24',
            3: '25–34',
            4: '35–44',
            5: '45–54',
            6: '55–64',
            7: '65–74',
            8: '75 years and over'
        }
        age_group = age_groups[min(filter(lambda x: x >= 1, age_groups.keys()), key=lambda x: abs(x - age))]
        if pd.notnull(row['smoke']):
            return row['smoke']
        elif sex == 1:
            return float(rates_df.loc[rates_df['Age Group'] == age_group, 'Males (%)'].values[0])
        elif sex == 0:
            return float(rates_df.loc[rates_df['Age Group'] == age_group, 'Females (%)'].values[0])
        else:
            return None

    def get_smoking_rate2(row, rates_dict):
        age = row['age']
        sex = row['sex']
        if age <= 24:
            group = "18–24"
        elif age <= 44:
            group = "25–44"
        elif age <= 64:
            group = "45–64"
        else:
            group = "and"
        if pd.notnull(row['smoke']):
            return row['smoke']
        elif sex == 0:
            smoke = rates_dict.get(group)
        else:
            smoke = rates_dict.get(group) * rates_dict.get('male') / rates_dict.get('female')
        return smoke

    # Read tables into pandas DataFrames
    df_max_fe = pd.read_sql_table(TABLE_NAMES['spark_fe'], con=CONN_URI)
    df_product_fe = pd.read_sql_table(TABLE_NAMES['pandas_fe'], con=CONN_URI)

    # Merge DataFrames
    df_merged = pd.merge(df_max_fe, df_product_fe, on='common_column', how='inner')

    # Apply functions to calculate smoking rates
    df_merged['smoking_rate1'] = df_merged.apply(get_smoking_rate1, rates_df=smoking_rates1, axis=1)
    df_merged['smoking_rate2'] = df_merged.apply(get_smoking_rate2, rates_dict=smoking_rates2, axis=1)

    # Convert to Spark DataFrame
    spark = SparkSession.builder.appName("MergeData").getOrCreate()
    spark_df = spark.createDataFrame(df_merged)

    # Write to database
    spark_df.write.format("jdbc").option("url", CONN_URI).option("dbtable", TABLE_NAMES['merged_data']).mode("overwrite").save()

    # Stop SparkSession
    spark.stop()

@from_table_to_df(TABLE_NAMES['original_data'], None)
def clean_data_func(**kwargs):
    """
    Data cleaning: drop none, remove outliers based on z-scores
    apply label encoding on categorical variables: assumption is that every string column is categorical
    """
    import pandas as pd
    from sklearn.preprocessing import LabelEncoder

    data_df = kwargs['dfs']

    # Drop rows with missing values
    data_df = data_df.dropna()

    # Remove outliers using Z-score 
    numeric_columns = [v for v in data_df.select_dtypes(include=['float64', 'int64']).columns if v != PARAMS['ml']['labels']]
    for column in numeric_columns:
        values = (data_df[column] - data_df[column].mean()).abs() / data_df[column].std() - PARAMS['ml']['outliers_std_factor']
        data_df = data_df[values < PARAMS['ml']['tolerance']]

    # Label encoding
    label_encoder = LabelEncoder()
    string_columns = [v for v in data_df.select_dtypes(exclude=['float64', 'int64']).columns if v != PARAMS['ml']['labels']]
    for v in string_columns:
        data_df[v + ENCODED_SUFFIX] = label_encoder.fit_transform(data_df[v])

    return {
        'dfs': [
            {'df': data_df, 
             'table_name': TABLE_NAMES['clean_data_pandas']
             }]
        }

@from_table_to_df(TABLE_NAMES['clean_data_pandas'], None)
def feature_engineering_func(**kwargs):
    """
    Perform feature engineering on the cleaned data.
    """
    import pandas as pd

    df = kwargs['dfs']

    # Create new features that are products of all pairs of features
    features = [v for v in df.select_dtypes(include=['float64', 'int64']).columns if v != PARAMS['ml']['labels']]
    new_features_df = pd.DataFrame()
    for i in range(len(features)):
        for j in range(i+1, len(features)):
            new_features_df['max_'+features[i]+'_'+features[j]] = df[[features[i], features[j]]].max(axis=1)

    return {
        'dfs': [
            {'df': pd.concat([new_features_df,df]), 
             'table_name': TABLE_NAMES['pandas_fe']
             }]
        }

@from_table_to_df(TABLE_NAMES['pandas_fe'], None)
def train_logistic_regression_func(**kwargs):
    """
    Train a logistic regression model on the feature-engineered data.
    """
    import pandas as pd
    from sklearn.linear_model import LogisticRegression
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import accuracy_score

    df = kwargs['dfs']

    # Prepare data for training
    X = df.drop(columns=[PARAMS['ml']['labels']])
    y = df[PARAMS['ml']['labels']]

    # Split the data into training and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=PARAMS['ml']['train_test_ratio'], random_state=42)

    # Train the logistic regression model
    lr_model = LogisticRegression()
    lr_model.fit(X_train, y_train)

    # Evaluate the logistic regression model
    y_pred_lr = lr_model.predict(X_test)
    lr_accuracy = accuracy_score(y_test, y_pred_lr)
    print(f"Logistic Regression Model accuracy: {lr_accuracy}")

    return {'accuracy': lr_accuracy}

@from_table_to_df(TABLE_NAMES['pandas_fe'], None)
def train_decision_tree_func(**kwargs):
    """
    Train a decision tree classifier on the feature-engineered data.
    """
    import pandas as pd
    from sklearn.tree import DecisionTreeClassifier
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import accuracy_score

    df = kwargs['dfs']

    # Prepare data for training
    X = df.drop(columns=[PARAMS['ml']['labels']])
    y = df[PARAMS['ml']['labels']]

    # Split the data into training and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=PARAMS['ml']['train_test_ratio'], random_state=42)

    # Train the decision tree model
    dt_model = DecisionTreeClassifier()
    dt_model.fit(X_train, y_train)

    # Evaluate the decision tree model
    y_pred_dt = dt_model.predict(X_test)
    dt_accuracy = accuracy_score(y_test, y_pred_dt)
    print(f"Decision Tree Classifier Model accuracy: {dt_accuracy}")

    return {'accuracy': dt_accuracy}

#@from_table_to_df(TABLE_NAMES['original_data'], None)
def clean_data_spark_func(**kwargs):
    """
    Data cleaning: drop none, remove outliers based on z-scores
    apply label encoding on categorical variables: assumption is that every string column is categorical
    """
    spark = create_spark_session()

    conn_uri = f"{PARAMS['db']['db_alchemy_driver']}://{PARAMS['db']['username']}:{PARAMS['db']['password']}@{PARAMS['db']['host']}:{PARAMS['db']['port']}/{PARAMS['db']['db_name']}"
    data_df = spark.read.format("jdbc").option("url", conn_uri).option("dbtable", TABLE_NAMES['original_data']).load()
    
    # Drop rows with missing values
    data_df = data_df.dropna()

    # Remove outliers using Z-score
    numeric_columns = [v for v in data_df.columns if data_df.schema[v].dataType in ['FloatType', 'DoubleType', 'IntegerType', 'LongType'] and v != PARAMS['ml']['labels']]
    for column in numeric_columns:
        mean_val = data_df.select(_mean(col(column))).collect()[0][0]
        stddev_val = data_df.select(_stddev(col(column))).collect()[0][0]
        data_df = data_df.filter((_abs(col(column) - mean_val) / stddev_val) < PARAMS['ml']['tolerance'])

    # Write the cleaned data back to the database
    data_df.write.format("jdbc").option("url", conn_uri).option("dbtable", TABLE_NAMES['clean_data_spark']).mode("overwrite").save()
    
    spark.stop()

@from_table_to_df(TABLE_NAMES['clean_data_spark'], None)
def feature_engineering_spark_func(**kwargs):
    """
    Perform feature engineering in Spark on the cleaned data.
    """
    spark = SparkSession.builder.appName("FeatureEngineering").getOrCreate()

    # Load cleaned data from database
    conn_uri = f"{PARAMS['db']['db_alchemy_driver']}://{PARAMS['db']['username']}:{PARAMS['db']['password']}@{PARAMS['db']['host']}:{PARAMS['db']['port']}/{PARAMS['db']['db_name']}"
    df = spark.read.format("jdbc").option("url", conn_uri).option("dbtable", TABLE_NAMES['clean_data']).load()

    # Select numeric features excluding the label
    numeric_features = [f.name for f in df.schema.fields if isinstance(f.dataType, DoubleType) and f.name != PARAMS['ml']['labels']]

    # Create new features that are products of all pairs of features
    for i in range(len(numeric_features)):
        for j in range(i + 1, len(numeric_features)):
            feature_name = f"{numeric_features[i]}*{numeric_features[j]}"
            df = df.withColumn(feature_name, col(numeric_features[i]) * col(numeric_features[j]))

    # Write the feature-engineered data back to the database
    df.write.format("jdbc").option("url", conn_uri).option("dbtable", TABLE_NAMES['spark_fe']).mode("overwrite").save()

    spark.stop()

#@from_table_to_df(TABLE_NAMES['clean_data_spark'], None)
def train_logistic_regression_spark_func(**kwargs):
    """
    Train a logistic regression model in Spark on the feature-engineered data.
    """
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.evaluation import MulticlassClassificationEvaluator
    
    spark = create_spark_session()
    
    # Load feature-engineered data from database
    conn_uri = f"{PARAMS['db']['db_alchemy_driver']}://{PARAMS['db']['username']}:{PARAMS['db']['password']}@{PARAMS['db']['host']}:{PARAMS['db']['port']}/{PARAMS['db']['db_name']}"
    df = spark.read.format("jdbc").option("url", conn_uri).option("dbtable", TABLE_NAMES['clean_data_spark']).load()

    # Prepare data for training
    feature_columns = [col for col in df.columns if col != PARAMS['ml']['labels']]
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    df = assembler.transform(df)

    # Split the data into training and test sets
    train_df, test_df = df.randomSplit([PARAMS['ml']['train_test_ratio'], 1 - PARAMS['ml']['train_test_ratio']], seed=42)

    # Train the logistic regression model
    lr = LogisticRegression(labelCol=PARAMS['ml']['labels'], featuresCol='features')
    lr_model = lr.fit(train_df)

    # Evaluate the logistic regression model
    predictions_lr = lr_model.transform(test_df)
    evaluator = MulticlassClassificationEvaluator(labelCol=PARAMS['ml']['labels'], predictionCol="prediction", metricName="accuracy")
    lr_accuracy = evaluator.evaluate(predictions_lr)
    print(f"Logistic Regression Model accuracy: {lr_accuracy}")
    
    spark.stop()

#@from_table_to_df(TABLE_NAMES['clean_data_spark'], None)
def train_decision_tree_spark_func(**kwargs):
    """
    Train a decision tree model in Spark on the feature-engineered data.
    """
    from pyspark.ml.classification import DecisionTreeClassifier
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.evaluation import MulticlassClassificationEvaluator
    
    spark = create_spark_session()
    
    # Load feature-engineered data from database
    conn_uri = f"{PARAMS['db']['db_alchemy_driver']}://{PARAMS['db']['username']}:{PARAMS['db']['password']}@{PARAMS['db']['host']}:{PARAMS['db']['port']}/{PARAMS['db']['db_name']}"
    df = spark.read.format("jdbc").option("url", conn_uri).option("dbtable", TABLE_NAMES['clean_data_spark']).load()

    # Prepare data for training
    feature_columns = [col for col in df.columns if col != PARAMS['ml']['labels']]
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    df = assembler.transform(df)

    # Split the data into training and test sets
    train_df, test_df = df.randomSplit([PARAMS['ml']['train_test_ratio'], 1 - PARAMS['ml']['train_test_ratio']], seed=42)

    # Train the decision tree model
    dt = DecisionTreeClassifier(labelCol=PARAMS['ml']['labels'], featuresCol='features')
    dt_model = dt.fit(train_df)

    # Evaluate the decision tree model
    predictions_dt = dt_model.transform(test_df)
    evaluator = MulticlassClassificationEvaluator(labelCol=PARAMS['ml']['labels'], predictionCol="prediction", metricName="accuracy")
    dt_accuracy = evaluator.evaluate(predictions_dt)
    print(f"Decision Tree Model accuracy: {dt_accuracy}")
    
    spark.stop()

# Function to scrape data from the web
def scrape_data_func(**kwargs):
    smokingrates1 = extract_tables()
    smokingrates2 = extract_cdc()
    kwargs['ti'].xcom_push(key='smokingrates1', value=smokingrates1)
    kwargs['ti'].xcom_push(key='smokingrates2', value=smokingrates2)

# Function to merge scraped data with feature-engineered datasets
def merge_scraped_data_func(**kwargs):
    smokingrates1 = kwargs['ti'].xcom_pull(key='smokingrates1', task_ids='scrape_data')
    smokingrates2 = kwargs['ti'].xcom_pull(key='smokingrates2', task_ids='scrape_data')

    # Assuming df_pandas contains the feature-engineered dataset
    df_pandas = pd.read_sql_table(TABLE_NAMES['train_data'], con=CONN_URI)

    # Renaming columns from the scraped data to avoid duplicates
    smokingrates1_renamed = smokingrates1.add_suffix('_scraped1')
    smokingrates2_renamed = smokingrates2.add_suffix('_scraped2')

    # Merging the datasets
    merged_df = pd.merge(df_pandas, smokingrates1_renamed, on='common_column', how='left')
    merged_df = pd.merge(merged_df, smokingrates2_renamed, on='common_column', how='left')

    # Convert pandas DataFrame back to Spark DataFrame
    spark = create_spark_session()
    spark_df = spark.createDataFrame(merged_df)

    # Write the merged data to the database
    spark_df.write.format("jdbc").option("url", conn_uri).option("dbtable", TABLE_NAMES['merged_data']).mode("overwrite").save()
    spark.stop()

@from_table_to_df(TABLE_NAMES['pandas_fe'], None)
def train_logistic_regression_func(**kwargs):
    """
    Train a logistic regression model on the feature-engineered data.
    """
    df = kwargs['dfs']
    import pandas as pd
    from sklearn.model_selection import train_test_split
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import accuracy_score

    Y = df[PARAMS['ml']['labels']]
    X = df.drop(PARAMS['ml']['labels'], axis=1)
    
    X_train, X_val, y_train, y_val = train_test_split(X, Y, test_size=PARAMS['ml']['train_test_ratio'], random_state=42)

    # Create an instance of Logistic Regression model
    model = LogisticRegression()

    # Train the model
    model.fit(X_train, y_train)

    # Make predictions on the val set
    y_pred = model.predict(X_val)

    # Calculate the accuracy of the model
    accuracy = accuracy_score(y_val, y_pred)
    print("Accuracy:", accuracy)        

    return accuracy

@from_table_to_df([TABLE_NAMES['pandas_fe'],TABLE_NAMES['clean_data_pandas']], None)
def train_decision_tree_func(**kwargs):
    """
    Train a decision tree classifier on the feature-engineered data.
    """
    import pandas as pd
    from sklearn.model_selection import train_test_split
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import accuracy_score

    dfs = kwargs['dfs']
    org_df = dfs[1]
    fe_df = dfs[0]

    # combine dataframes
    df = pd.concat([org_df, fe_df], axis=1)

    # Split the data into training and validation sets
    string_columns = [v for v in df.select_dtypes(exclude=['float64', 'int64']).columns if v != PARAMS['ml']['labels']]
    df = df.drop(string_columns, axis=1)

    Y = df[PARAMS['ml']['labels']]
    X = df.drop(PARAMS['ml']['labels'], axis=1)
    
    X_train, X_val, y_train, y_val = train_test_split(X, Y, test_size=PARAMS['ml']['train_test_ratio'], random_state=42)

    # Create an instance of Logistic Regression model
    model = DecisionTreeClassifier()

    # Train the model
    model.fit(X_train, y_train)

    # Make predictions on the val set
    y_pred = model.predict(X_val)

    # Calculate the accuracy of the model
    accuracy = accuracy_score(y_val, y_pred)
    print("Accuracy:", accuracy)        

    return accuracy


from sklearn.metrics import accuracy_score

def find_best_model(**kwargs):
    """
    Function to determine the best model based on evaluation metrics.
    """
    # Retrieve the evaluation results from XCom
    ti = kwargs['ti']
    lr_accuracy = ti.xcom_pull(task_ids='train_logistic_regression_pandas')
    dt_accuracy = ti.xcom_pull(task_ids='train_decision_tree_pandas')
    lr_accuracy_spark = ti.xcom_pull(task_ids='train_logistic_regression_spark')
    dt_accuracy_spark = ti.xcom_pull(task_ids='train_decision_tree_spark')
    lr_accuracy_scraped = ti.xcom_pull(task_ids='train_logistic_regression_scraped')
    dt_accuracy_scraped = ti.xcom_pull(task_ids='train_decision_tree_scraped')

    # Determine the best model based on the highest accuracy
    best_model = max([
        ('Logistic Regression (Pandas)', lr_accuracy),
        ('Decision Tree (Pandas)', dt_accuracy),
        ('Logistic Regression (Spark)', lr_accuracy_spark),
        ('Decision Tree (Spark)', dt_accuracy_spark),
        ('Logistic Regression (Scraped)', lr_accuracy_scraped),
        ('Decision Tree (Scraped)', dt_accuracy_scraped)
    ], key=lambda x: x[1])

    print(f"The best model is: {best_model[0]} with accuracy: {best_model[1]}")

def evaluate_on_test(**kwargs):
    """
    Function to evaluate the best model on the test data.
    """
    # Retrieve the best model from XCom
    ti = kwargs['ti']
    best_model = ti.xcom_pull(task_ids='find_best_model')

    # Evaluate the best model on the test data
    if "Logistic Regression" in best_model[0]:
        model = LogisticRegression()
    else:
        model = DecisionTreeClassifier()

    # Train the model on the entire training data
    # Assuming you have the training data available in some form
    # Replace 'X_train' and 'y_train' with your actual training data
    model.fit(X_train, y_train)

    # Evaluate the model on the test data
    # Assuming you have the test data available in some form
    # Replace 'X_test' and 'y_test' with your actual test data
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    print(f"Accuracy of the best model on test data: {accuracy}")

    # Optionally, you can save the model for future use
    # model.save('best_model.pkl')

    # You may also want to log or store the evaluation results for further analysis



# Define the DAG
dag = DAG(
    'hw4-cindy',
    default_args=default_args,
    description='heart disease prediction',
    schedule_interval='@daily',
    tags=["de300"]
)

# Define the tasks
drop_tables = PostgresOperator(
    task_id='drop_tables',
    postgres_conn_id=PARAMS['db']['db_connection'],
    sql=f"""
    DROP TABLE IF EXISTS {TABLE_NAMES['original_data']};
    DROP TABLE IF EXISTS {TABLE_NAMES['clean_data_spark']};
    DROP TABLE IF EXISTS {TABLE_NAMES['clean_data_pandas']};
    DROP TABLE IF EXISTS {TABLE_NAMES['train_data']};
    DROP TABLE IF EXISTS {TABLE_NAMES['test_data']};
    DROP TABLE IF EXISTS {TABLE_NAMES['normalization_data']};
    DROP TABLE IF EXISTS {TABLE_NAMES['merged_data']};
    DROP TABLE IF EXISTS {TABLE_NAMES['spark_fe']};
    DROP TABLE IF EXISTS {TABLE_NAMES['pandas_fe']};
    """,
    dag=dag
)

add_data_to_table = PythonOperator(
    task_id='add_data_to_table',
    python_callable=add_data_to_table_func,
    dag=dag
)

# Pandas pipeline
clean_data_pandas = PythonOperator(
    task_id='clean_data_pandas',
    python_callable=clean_data_func,
    provide_context=True,
    dag=dag
)

feature_engineering_pandas = PythonOperator(
    task_id='feature_engineering_pandas',
    python_callable=feature_engineering_func,
    provide_context=True,
    dag=dag
)

train_logistic_regression_scraped = PythonOperator(
    task_id='train_logistic_regression_scraped',
    python_callable=train_logistic_regression_func,
    provide_context=True,
    dag=dag
)

train_decision_tree_scraped = PythonOperator(
    task_id='train_decision_tree_scraped',
    python_callable=train_decision_tree_func,
    provide_context=True,
    dag=dag
)


train_logistic_regression_pandas = PythonOperator(
    task_id='train_logistic_regression_pandas',
    python_callable=train_logistic_regression_func,
    provide_context=True,
    dag=dag
)

train_decision_tree_pandas = PythonOperator(
    task_id='train_decision_tree_pandas',
    python_callable=train_decision_tree_func,
    provide_context=True,
    dag=dag
)

# Spark pipeline
clean_data_spark = PythonOperator(
    task_id='clean_data_spark',
    python_callable=clean_data_spark_func,
    provide_context=True,
    dag=dag
)

feature_engineering_spark = PythonOperator(
    task_id='feature_engineering_spark',
    python_callable=feature_engineering_spark_func,
    provide_context=True,
    dag=dag
)

train_logistic_regression_spark = PythonOperator(
    task_id='train_logistic_regression_spark',
    python_callable=train_logistic_regression_spark_func,
    provide_context=True,
    dag=dag
)

train_decision_tree_spark = PythonOperator(
    task_id='train_decision_tree_spark',
    python_callable=train_decision_tree_spark_func,
    provide_context=True,
    dag=dag
)

scrape_data_task = PythonOperator(
    task_id='scrape_data',
    python_callable=scrape_data_func,
    provide_context=True,
    dag=dag,
)

merge_scraped_data_task = PythonOperator(
    task_id='merge_scraped_data',
    python_callable=merge_scraped_data_func,
    provide_context=True,
    dag=dag,
)

find_best_model = PythonOperator(
    task_id='find_best_model',
    python_callable=find_best_model,
    provide_context=True,
    dag=dag
)

evaluate_on_test = PythonOperator(
    task_id='evaluate_on_test',
    python_callable=evaluate_on_test,
    provide_context=True,
    dag=dag
)

# Define the task dependencies
drop_tables >> add_data_to_table

# Pandas pipeline dependencies
add_data_to_table >> [clean_data_pandas, clean_data_spark]

clean_data_pandas >> feature_engineering_pandas >> [train_logistic_regression_pandas, train_decision_tree_pandas]

# Spark pipeline dependencies
clean_data_spark >> feature_engineering_spark >> [train_logistic_regression_spark, train_decision_tree_spark]

# scrape data and merge
scrape_data_task >> merge_scraped_data_task >> [train_logistic_regression_scraped, train_decision_tree_scraped] 

# Dummy operators to mark end of branches
end_pandas = DummyOperator(task_id='end_pandas', dag=dag)
end_spark = DummyOperator(task_id='end_spark', dag=dag)

# Model evaluation and selection
[train_logistic_regression_pandas, train_decision_tree_pandas, train_logistic_regression_spark, train_decision_tree_spark, train_logistic_regression_scraped, train_decision_tree_scraped] >> find_best_model >> evaluate_on_test
