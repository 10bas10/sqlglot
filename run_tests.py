from sqlglot.vlk.batch import batchTranspile
from sqlglot.vlk.repoinventory import Database

database_types = (
    Database.Type.TRANSFORM,
    Database.Type.REFERENCE,
    Database.Type.STAGING,
    Database.Type.STG_ARCHIVE,
)
database_filter = lambda x: "inflationbreaker" in x.lower() or x.lower() == "reference"

batch = batchTranspile(
    root="C:/Users/USEFRA/source/repos/DWH-VLK",
    scope=database_types,
    filter=database_filter,
    view_metadata_csv="metadata/view_metadata.csv",
    db_prefix="vl_dwh_",
)

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
SELECT	CONVERT(nvarchar(50),	'customer_nps') AS meta_nm_mapping
		,CONVERT(nvarchar(10),	@meta_nr_mapping) AS meta_nr_mapping
		,CONVERT(nvarchar(255),	'PRA|' + STAGING_InflationBreaker.IFB.CUSTOMER.ENTITY_ID + '|' + STAGING_InflationBreaker.IFB.CUSTOMER.CUSTOMER_ID+ CASE WHEN Periode.CD_Periode = 'daily' THEN '' ELSE '|' + Periode.CD_Periode END) AS [ID_persoon_rapportage]
		,CONVERT(nvarchar(40),	STAGING_InflationBreaker.IFB.customer.[entity_id]) AS [CD_entiteit]
		,CONVERT(nvarchar(3),	'PRA') AS [TY_persoon_rapportage]
		,CONVERT(nvarchar(40),	Periode.CD_periode) AS [CD_periode]
		,CONVERT(nvarchar(255),	'NPS|' + STAGING_InflationBreaker.IFB.CUSTOMER.ENTITY_ID + '|' + STAGING_InflationBreaker.IFB.CUSTOMER.CUSTOMER_ID+ CASE WHEN Periode.CD_Periode = 'daily' THEN '' ELSE '|' + Periode.CD_Periode END) AS [CD_persoon_rapportage_id]
		,CONVERT(nvarchar(255),	STAGING_InflationBreaker.IFB.customer.[customer_id]) AS [NR_persoon_rapportage]
		,CONVERT(nvarchar(255),	'') AS [NR_persoon_rapportage_bron]
		/*============================ SNAPSHOT DATE =========================================*/
		/*  The query selects multiple snapshots. The snapshot date identifies the snapshot.  */
		,CONVERT(date,		STAGING_InflationBreaker.IFB.CUSTOMER.meta_dt_snapshot) AS [meta_dt_snapshot]
/*============================ MAPPING CRITERIA: BASE SELECTION ==============================*/
FROM STAGING_InflationBreaker.IFB.CUSTOMER
INNER JOIN STAGING_InflationBreaker.IFB.controle
ON 1=1
AND STAGING_InflationBreaker.IFB.CUSTOMER.meta_dt_snapshot = STAGING_InflationBreaker.IFB.controle.meta_dt_snapshot
AND STAGING_InflationBreaker.IFB.CUSTOMER.[entity_id] = STAGING_InflationBreaker.IFB.controle.[entity_id]
AND STAGING_InflationBreaker.IFB.controle.[table] = 'CUSTOMER'
OUTER APPLY REFERENCE.TRN.GetPeriod('IFB', STAGING_InflationBreaker.IFB.controle.dt_reporting_period+'01') as Periode

WHERE 1=1
AND STAGING_InflationBreaker.IFB.CUSTOMER.COUNTERPARTY_CLASSIFICATION IN ('510','520')
/*=================================== SNAPSHOT SELECTION ======================================*/
AND STAGING_InflationBreaker.IFB.CUSTOMER.[meta_dt_snapshot] > @meta_dt_snapshot
/*------------------------------------- END OF FUNCTION ---------------------------------------*/
"""

expr = sqlglot.parse_one(definition, read="tsql")

# Get the fully qualified table reference
expr = qualify_tables(expression=expr, catalog="transform_asr")

# Transform values to a union in case of tuples with references to outer columns
expr = transform(expr, values_to_union)
# print([expr])
# Normalize table references
expr = transform(expr, normalize_table_references)

# Normalize column references
expr = transform(expr, normalize_column_references)

# Normalize table aliases
expr = transform(expr, normalize_table_aliases)

# Normalize column aliases
expr = transform(expr, normalize_column_aliases)

# Annotate UDTF attributes with a datatype
expr = transform(expr, annotate_udtf)

# Get the fully qualified column names
expr = qualify_columns(expression=expr, schema=batch.schema)

# Get the datatypes of columns
expr = annotate_types(expr, schema=batch.schema)

# Translate text concat functions
print([expr])
expr = transform(expr, translate_add_to_dpipe)

# Change database and table references according to hive tables
expr = transform(expr, translate_db_and_table, batch.db_prefix)

# Change database and function references according to hive structure
expr = transform(expr, translate_udtf, "vl_dwh_")

# Transform correlated subqueries to a regular left join, lateral or cross join
expr = transform(expr, subquery_to_join)

# Set VLK custom generator functions
dbx = sqlglot.Dialect.get_or_raise("databricks")()
dbx_tfm = dbx.Generator.TRANSFORMS

output = dbx.generate(expr, pretty=True)

# print([expr])
print(output)
