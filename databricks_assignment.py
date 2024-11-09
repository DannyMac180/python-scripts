# Please provide your code answer for Question 1 here
# Write the Data Frame into a CSV and load it into the `data/` folder
import pyspark.sql.functions as f
from pyspark.sql import SparkSession

# create a spark session
spark = SparkSession.builder.appName("databricks_de").getOrCreate()
schema = ["sid", "id", "position", "created_at", "created_meta", "updated_at", "updated_meta", "meta", "year", "first_name", "county", "sex", "count"]
baby_names_path = 'data/databricks_baby_names.json'
# Do not edit the code above this line.
########################################

### Please provide your code answer for Question 1 by implementing the read_json_and_flatten_data() and parse_dataframe_with_schema() functions below.

# Do not modify the function declarations below. Implement them based on the provided specifications.
def read_json_and_flatten_data(spark, baby_names_path):
    """
    Reads the JSON data from the provided path and pulls all columns in the nested data column to top level.
    
    Parameters:
    - spark: The SparkSession object.
    - baby_names_path: Path to the JSON file containing baby names data.

    Returns:
    - A DataFrame.
    """
    # Read the JSON data
    df_json = spark.read.option("multiLine", True).json(baby_names_path)
    
    # Extract the column names from the 'meta.view.columns' field
    columns = df_json.select("meta.view.columns").first()[0]
    column_names = [col["name"] for col in columns]
    
    # Explode the 'data' field
    df_exploded = df_json.select(f.explode(df_json.data).alias("data_row"))
    
    # Flatten the data
    df_flattened = df_exploded.select([f.col("data_row").getItem(i).alias(column_names[i]) for i in range(len(column_names))])
    
    return df_flattened

def parse_dataframe_with_schema(df_processed, schema):
    """
    Parses the DataFrame returned by read_json_and_flatten_data for output to CSV based on the provided schema.
    
    Parameters:
    - df_processed: DataFrame returned from read_json_and_flatten_data.
    - schema: Schema to follow for the output CSV.

    Returns:
    - A DataFrame processed based on the provided schema.
    """
    # Select only the columns specified in the schema
    df_selected = df_processed.select(schema)
    
    # Cast 'year' and 'count' columns to integer data types
    df_casted = df_selected.withColumn("year", df_selected["year"].cast("integer")) \
                           .withColumn("count", df_selected["count"].cast("integer"))
    
    # Replace nulls with empty strings
    df_casted = df_casted.fillna('')
    
    return df_casted

########################################
# Do not edit the code below this line
df_processed = read_json_and_flatten_data(spark, baby_names_path)
df = parse_dataframe_with_schema(df_processed, schema)
df.toPandas().to_csv('data/baby_names.csv', index=False)
spark.stop()

# Please provide your brief written description of your code here. (Commented out)
# In the read_json_and_flatten_data() function, we read the JSON data with multiLine option enabled.
# We extract the column names from 'meta.view.columns' to ensure the data columns are correctly mapped.
# Then, we explode the 'data' array and flatten it by assigning the extracted column names.
# In the parse_dataframe_with_schema() function, we select only the columns specified in the schema,
# cast 'year' and 'count' to integer types, replace nulls with empty strings, and return the processed DataFrame.
