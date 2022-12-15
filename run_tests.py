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
)

from sqlglot.dialects.databricks import Databricks

definition = """
SELECT	CONVERT(nvarchar(50),	'equity') AS meta_nm_mapping
		,CONVERT(nvarchar(10),	@meta_nr_mapping) AS meta_nr_mapping
		,CONVERT(nvarchar(255),	'IPO|BDP|'+ STAGING_Dochtersheets.KCO.EQUITY.ENTITY_ID  +'|'+ STAGING_Dochtersheets.KCO.EQUITY.POSITION_KEY  + '|INS|'  +  STAGING_Dochtersheets.KCO.EQUITY.SECURITY_ID+ CASE WHEN Periode.CD_Periode = 'daily' THEN '' ELSE '|' + Periode.CD_Periode END) AS [ID_instrument_positie]
		,CONVERT(nvarchar(40),	STAGING_Dochtersheets.KCO.equity.[entity_id]) AS [CD_entiteit]
		,CONVERT(nvarchar(3),	'IPO') AS [TY_instrument_positie]
		,CONVERT(nvarchar(40),	Periode.CD_periode) AS [CD_periode]
		,CONVERT(nvarchar(255),	'BDP|'+  STAGING_Dochtersheets.KCO.EQUITY.ENTITY_ID + '|FEQ|' + STAGING_Dochtersheets.KCO.EQUITY.SAFEKEEPING_ACCOUNT + CASE WHEN Periode.CD_Periode = 'daily' THEN '' ELSE '|' + Periode.CD_Periode END) AS [ID_product]
		,CONVERT(nvarchar(255),	'INS|' + STAGING_Dochtersheets.KCO.EQUITY.ENTITY_ID  + '|' + STAGING_Dochtersheets.KCO.EQUITY.SECURITY_ID + CASE WHEN Periode.CD_Periode = 'daily' THEN '' ELSE '|' + Periode.CD_Periode END) AS [ID_instrument]
		,CONVERT(nvarchar(40),	STAGING_Dochtersheets.KCO.equity.[country_of_risk]) AS [CD_bewaarland_1]
		,CONVERT(decimal(19,6),	STAGING_Dochtersheets.KCO.equity.[quantity]) AS [AN_positie]
		,CONVERT(decimal(19,6),	STAGING_Dochtersheets.KCO.EQUITY.ACTUAL_AMOUNT
										) AS [BD_positie_waarde]
		,CONVERT(nvarchar(40),	STAGING_Dochtersheets.KCO.equity.[measurement_category]) AS [CD_waarderingscategorie]
		,CONVERT(decimal(19,6),	STAGING_Dochtersheets.KCO.equity.[unrealised_gains_losses_period]) AS [BD_niet_gerealiseerde_venw_op_periode]
		,CONVERT(decimal(19,6),	STAGING_Dochtersheets.KCO.equity.[unrealised_gains_losses_cum]) AS [BD_niet_gerealiseerde_venw_cumulatief]
		,CONVERT(decimal(19,6),	NULL  /* Niet gemapt */) AS [BD_nominaal]
		,CONVERT(bit,			0) AS [IS_pro_rata]
		,CONVERT(nvarchar(40),	'NVT'  /* Niet gemapt */) AS [CD_type_positie_1]
		,CONVERT(nvarchar(40),	NULL  /* Niet gemapt */) AS [CD_type_positie_0]
		/*============================ SNAPSHOT DATE =========================================*/
		/*  The query selects multiple snapshots. The snapshot date identifies the snapshot.  */
		,CONVERT(date,		STAGING_Dochtersheets.KCO.equity.meta_dt_snapshot) AS [meta_dt_snapshot]
/*============================ MAPPING CRITERIA: BASE SELECTION ==============================*/
FROM STAGING_Dochtersheets.KCO.equity
INNER JOIN STAGING_Dochtersheets.KCO.controle
ON 1=1
AND STAGING_Dochtersheets.KCO.equity.meta_dt_snapshot = STAGING_Dochtersheets.KCO.controle.meta_dt_snapshot
AND STAGING_Dochtersheets.KCO.equity.[entity_id] = STAGING_Dochtersheets.KCO.controle.[entity_id]
AND STAGING_Dochtersheets.KCO.controle.[table] = 'equity'
OUTER APPLY REFERENCE.TRN.GetPeriod('DCH', STAGING_Dochtersheets.KCO.controle.dt_reporting_period+'01') as Periode


where STAGING_Dochtersheets.KCO.EQUITY.SECURITY_ID is not null and STAGING_Dochtersheets.KCO.EQUITY.SAFEKEEPING_ACCOUNT is not null
"""

expr = sqlglot.parse_one(definition, read="tsql")

# Get the fully qualified table reference
expr = qualify_tables(expression=expr, catalog="transform_asr")

# Normalize table references
expr = expr.transform(normalize_table_references)

# Normalize column references
expr = expr.transform(normalize_column_references)

# Normalize table aliases
expr = expr.transform(normalize_table_aliases)

# Normalize column aliases
expr = expr.transform(normalize_column_aliases)

# # Annotate UDTF attributes with a datatype
expr = expr.transform(annotate_udtf)

# # # Get the fully qualified column names
# expr = qualify_columns(expression=expr, schema=batch.schema, check_unknown_tables=False)

# # Get the datatypes of columns
# expr = annotate_types(expr, schema=batch.schema)

# # Translate text concat functions
# expr = expr.transform(translate_add_to_dpipe)

# # Change database and table references according to hive tables
# expr = expr.transform(translate_db_and_table, batch.db_prefix)

# Change database and function references according to hive structure
expr = expr.transform(translate_udtf, "vl_dwh_")

# Transform correlated subqueries to a regular left join, lateral or cross join
expr = expr.transform(subquery_to_join)

# Transform values to a union in case of tuples with references to outer columns
expr = expr.transform(values_to_union)

# Set VLK custom generator functions
dbx = sqlglot.Dialect.get_or_raise("databricks")()
dbx_tfm = dbx.Generator.TRANSFORMS

output = dbx.generate(expr, pretty=True)

# print([expr])
print(output)
