import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.functions import col


# --------------------------------
# Functions to flatten nested JSON
# --------------------------------
def get_flattened_fields(schema, prefix):
    fields = []
    for field in schema.fields:
        name = prefix + '.' + field.name if prefix else field.name
        dtype = field.dataType
        if isinstance(dtype, ArrayType):
            dtype = dtype.elementType
        if isinstance(dtype, StructType):
            fields += get_flattened_fields(dtype, prefix=name)
        else:
            fields.append(name)
    return fields


def flatten(dataframe, prefix=None):
    schema = dataframe.schema
    fields = get_flattened_fields(schema, prefix)

    # Consider repeated columns only once
    field_dict = {}
    for field in fields:
        if "." in field and field.split(".")[1] not in field_dict:
            field_dict[field.split(".")[1]] = field
        elif "." not in field:
            field_dict[field] = field
    new_field_list = list(field_dict.values())

    # Get the 1st element of array in case of array types
    dataframe = dataframe.select(new_field_list)
    new_field_list = dataframe.columns
    for field in dataframe.schema.fields:
        if isinstance(field.dataType, ArrayType):
            index = new_field_list.index(field.name)
            new_field_list[index] = f"{field.name}[0] as {field.name}"
    dataframe = dataframe.selectExpr(new_field_list)

    return dataframe
# --------------------------------


spark = SparkSession.builder.appName("Spark-JSON").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Read JSON and flatten it
json_df = spark.read \
    .option("multiline", "true") \
    .json(r"C:\Users\Vicky\Minnie\json_data.json")
json_df = flatten(json_df)

# Read CSV
csv_df = spark.read \
    .options(header='true', inferSchema='true') \
    .csv(r"C:\Users\Vicky\Minnie\json_data.csv")

# The following line after the comments converts JSON schema to CSV schema
# This is done because JSON contains all String types. We need to convert numbers in the form
# of strings to int/long/float for correct comparison
json_df = json_df.select([col(column[0]).cast(column[1]).alias(column[0]) for column in csv_df.dtypes])

# Display converted JSON schema and JSON data
json_df.printSchema()
json_df.show(truncate=False)

spark.stop()
