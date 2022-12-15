from sqlglot.vlk.batch import batchTranspile
from sqlglot.vlk.repoinventory import Database

database_types = (
    Database.Type.TRANSFORM,
    Database.Type.REFERENCE,
    Database.Type.STAGING,
    Database.Type.STG_ARCHIVE,
)
database_filter = lambda x: "asr" in x.lower() or x.lower() == "reference"

# batch = batchTranspile(
#     root="c:/dev/repos/DWH-VLK",
#     scope=database_types,
#     filter=database_filter,
#     view_metadata_csv="metadata/view_metadata.csv",
#     db_prefix="vl_dwh_",
# )

import pyspark
from pyspark.sql import SparkSession
import sqlglot
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.qualify_tables import qualify_tables
from sqlglot.optimizer.qualify_columns import qualify_columns
from sqlglot.vlk.repoinventory import Repo
from sqlglot.vlk.transforms import (
    annotate_udtf,
    translate_add_to_dpipe,
    normalize_table_references,
    normalize_column_references,
    normalize_table_aliases,
    normalize_column_aliases,
    translate_db_and_table,
    translate_udtf,
    subquery_to_join,
    values_to_union,
	transform,
)

from sqlglot.dialects.databricks import Databricks

definition = """
SELECT 
 test
FROM (
	VALUES
	 ('test', 'test')
)x
"""

expr = sqlglot.parse_one(definition, read="tsql")

# Get the fully qualified table reference
expr = qualify_tables(expression=expr, catalog="transform_asr")

# Normalize table references
expr = transform(expr, normalize_table_references)

# Normalize column references
expr = transform(expr, normalize_column_references)

# Normalize table aliases
expr = transform(expr, normalize_table_aliases)

# Normalize column aliases
expr = transform(expr, normalize_column_aliases)

# # Annotate UDTF attributes with a datatype
expr = transform(expr, annotate_udtf)

# # # Get the fully qualified column names
# expr = qualify_columns(expression=expr, schema=batch.schema, check_unknown_tables=False)

# # Get the datatypes of columns
# expr = annotate_types(expr, schema=batch.schema)

# # Translate text concat functions
# expr = expr.transform(translate_add_to_dpipe)

# # Change database and table references according to hive tables
# expr = expr.transform(translate_db_and_table, batch.db_prefix)

# Change database and function references according to hive structure
expr = transform(expr, translate_udtf, "vl_dwh_")

# Transform correlated subqueries to a regular left join, lateral or cross join
expr = transform(expr, subquery_to_join)

# Transform values to a union in case of tuples with references to outer columns
expr = transform(expr, values_to_union)

# Set VLK custom generator functions
dbx = sqlglot.Dialect.get_or_raise("databricks")()
dbx_tfm = dbx.Generator.TRANSFORMS

output = dbx.generate(expr, pretty=True)

# print([expr])
print(output)
