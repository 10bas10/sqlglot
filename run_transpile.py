from sqlglot.vlk.batch import batchTranspile
from sqlglot.vlk.repoinventory import Database

database_types = (
    Database.Type.TRANSFORM,
    Database.Type.REFERENCE,
    Database.Type.STAGING,
    Database.Type.STG_ARCHIVE,
)
database_filter = None#lambda x: "bankview" in x.lower() or x.lower() == "reference"

batch = batchTranspile(
    root="c:/dev/repos/DWH-VLK",
    scope=database_types,
    filter=database_filter,
    view_metadata_csv="metadata/view_metadata.csv",
    db_prefix="vl_dwh_",
)

batch.transpile()
batch.create_source_tables()
batch.test_mappings()
batch.report()
