import csv
from tqdm import tqdm
import xml.dom.minidom as xmld
import re, os, glob
from pathlib import Path
from sqlglot.vlk.helpers import norm_id


class Repo:
    datatype_translate = {
        "NVARCHAR": "STRING",
        "VARCHAR": "STRING",
        "CHAR": "STRING",
        "NCHAR": "STRING",
        "NTEXT": "STRING",
        "DATETIME": "TIMESTAMP",
        "DATETIME2": "TIMESTAMP",
        "SMALLDATETIME": "TIMESTAMP",
        "MONEY": "DECIMAL",
        "UNIQUEIDENTIFIER": "STRING",
        "BIT": "BOOLEAN",
        "XML": "STRING",
        "TIME": "TIMESTAMP",
    }

    def __init__(self, root, scope, filter, db_prefix):
        self.root = root
        self.scope = scope
        self.filter = filter
        self.mappings = []
        self.databases = {}
        self.db_prefix = db_prefix
        self.databases = self.__collect()

        self.__process()

    def __str__(self):
        return f"{self.root}"

    def __collect(self):
        sqlDbs = {}
        for sqlproj in glob.glob(f"{self.root}/**/*.sqlproj", recursive=True):
            sqlDb = Database(repo=self, dbfile=sqlproj)
            if sqlDb.in_scope:
                sqlDbs[sqlDb.name] = sqlDb
        return sqlDbs

    def __process(self):
        for db in tqdm(iterable=self.databases.values(), desc="Retrieving objects"):
            db.collect_objects()

    def get_schema_dict(self, view_metadata_csv):
        schema = {
            dbkey: {
                schemakey: {
                    tablekey: {
                        columnkey: column.datatype for columnkey, column in table.columns.items()
                    }
                    for tablekey, table in schema.tables.items()
                }
                for schemakey, schema in db.schemas.items()
            }
            for dbkey, db in self.databases.items()
        }

        if view_metadata_csv:
            with open(
                os.path.join(os.path.dirname(os.path.realpath(__file__)), view_metadata_csv),
                mode="r",
                encoding="utf-8-sig",
            ) as f:
                data = csv.DictReader(f, delimiter=";")
                for row in data:
                    db = norm_id(row["table_database"])
                    tbl_schema = norm_id(row["table_schema"])
                    table = norm_id(row["table_name"])
                    column = norm_id(row["column_name"])
                    if db not in schema:
                        schema[db] = {}
                    if tbl_schema not in schema[db]:
                        schema[db][tbl_schema] = {}
                    if table not in schema[db][tbl_schema]:
                        schema[db][tbl_schema][table] = {}

                    schema[db][tbl_schema][table][column] = (
                        Repo.datatype_translate[row["data_type"]]
                        if row["data_type"] in Repo.datatype_translate
                        else row["data_type"]
                    )

        return schema


class Database:
    class Type:
        TRANSFORM = "Transform"
        DATAMART = "Datamart/Extractionmart"
        CALCULATOR = "Calculator"
        CALC_ARCHIVE = "Calculator Archive"
        REFERENCE = "CIP/REFERENCE"
        STAGING = "Staging"
        STG_ARCHIVE = "Staging Archive"
        OTHER = "Other databases"

    def __init__(self, repo, dbfile):
        self.dir = os.path.dirname(os.path.realpath(dbfile))
        self.file = dbfile
        self.repo = repo
        self.name = norm_id(os.path.splitext(os.path.basename(self.file))[0])
        self.type = None
        self.schemas = {}
        self.raw_udfs = []
        self.raw_procs = []
        self.raw_tables = []
        self.raw_schemas = []

        self.type = self.__get_db_type()

    def __str__(self):
        return f"{self.name}"

    def __collect_raw(self):
        doc = xmld.parse(self.file)

        builditems = doc.getElementsByTagName("Build")
        for builditem in builditems:
            buildobject = builditem.getAttribute("Include").lower().replace("\\", "/")

            if "/tables/" in buildobject:
                self.raw_tables.append(buildobject)
            elif "/stored procedures/" in buildobject and "usp_load_" in buildobject:
                self.raw_procs.append(buildobject)
            elif "/functions/" in buildobject:
                self.raw_udfs.append(buildobject)

        raw_objects = self.raw_tables + self.raw_procs + self.raw_udfs
        for schema in set(buildobject.split("/")[0] for buildobject in raw_objects):
            if schema not in ["dbo"]:
                self.raw_schemas.append(schema)

        return None

    def __get_db_type(self):
        dbname = self.name.lower()
        dbtype = self.Type.OTHER
        if dbname.startswith("transform_"):
            dbtype = self.Type.TRANSFORM
        elif dbname.startswith("dm_") or dbname.startswith("em_"):
            dbtype = self.Type.DATAMART
        elif dbname.startswith("calc_dwh_"):
            dbtype = self.Type.CALCULATOR
        elif dbname.startswith("calc_archive_"):
            dbtype = self.Type.CALC_ARCHIVE
        elif dbname in ["cip", "reference"]:
            dbtype = self.Type.REFERENCE
        elif dbname.startswith("staging_"):
            dbtype = self.Type.STAGING
        elif dbname.startswith("stg_archive_"):
            dbtype = self.Type.STG_ARCHIVE
        return dbtype

    def collect_objects(self):
        self.__collect_raw()

        for schema in self.raw_schemas:
            schema = norm_id(schema)
            self.schemas[schema] = Schema(db=self, name=schema)

        for table in self.raw_tables:
            table_path = os.path.join(self.dir, table)
            with open(table_path, "r", encoding="utf-8-sig") as f:
                table_def = f.read()

            table_ddl = re.search(r"\b(CREATE TABLE)\b", table_def, re.IGNORECASE)
            if table_ddl is None:
                continue

            table_properties = re.search(
                r"\b(CREATE TABLE)\b([ \t])+\[?([A-Za-z0-9_-]*)\]?.{1}\[?([A-Za-z0-9_-]+)\]?",
                table_def,
                re.IGNORECASE,
            )
            if table_properties is None:
                raise ValueError(f"File {table} cannot be matched on table name!")

            schema_name = norm_id(table_properties.group(3))
            schema_obj = self.schemas[schema_name]

            table_name = norm_id(table_properties.group(4))
            tbl = Table(schema=schema_obj, name=table_name)

            columns = re.findall(
                r"((\[+([% A-Za-z0-9_-]+)\]+)|([%A-Za-z0-9_-])+)[ \t]+(\[?([A-Za-z0-9]+)\]?)[ \t]*[()0-9, ]*[ \t]+((CONSTRAINT|DEFAULT)+(.)+)*(NOT )*(NULL)+",
                table_def,
                re.IGNORECASE,
            )
            if columns is None:
                raise ValueError(f"File {table} cannot be matched on columns!")
            cols = dict(
                zip(
                    (norm_id(column[2] if column[2] else column[0]) for column in columns),
                    (
                        Column(table=tbl, name=norm_id(column[2] if column[2] else column[0]), datatype=column[5].upper())
                        for column in columns
                    ),
                )
            )
            tbl.columns = cols

            schema_obj.tables[table_name] = tbl

        return None

    @property
    def in_scope(self):
        return self.type in self.repo.scope and (
            self.repo.filter is None or self.repo.filter(self.name)
        )


class Schema:
    def __init__(self, db, name):
        self.db = db
        self.name = name
        self.tables = {}

    def __str__(self):
        return f"{self.name}"


class Table:
    def __init__(self, schema, name):
        self.name = name
        self.schema = schema
        self.columns = {}
        self.mappings = {}

        self.__collect_mappings()

    def __str__(self):
        return f"{self.schema.db}.{self.schema.name}.{self.name}"

    def __collect_mappings(self):
        procs = list(
            proc
            for proc in self.schema.db.raw_procs
            if proc == f"{self.schema.name}/stored procedures/usp_load_{self.name}.sql"
        )

        if self.schema.db.type == Database.Type.TRANSFORM and not procs:
            raise ValueError(
                f"No mappings found for {self.schema.db.name}.{self.schema}.{self.name}"
            )

        # Currently only one SP, but in case of certain datamarts we can expect multiple
        for proc in procs:
            proc_file = os.path.join(self.schema.db.dir, proc)
            with open(proc_file, "r", encoding="utf-8-sig") as t:
                proc_def = t.read()
            udfs = re.findall(
                r"(SELECT)[0-9A-Za-z \t*\n]*(FROM)[ \t]+[\[]?([A-Z]+)[\]]?..[\[]?([A-Za-z_0-9-]+)[\]]?",
                proc_def,
                re.IGNORECASE,
            )
            if udfs is None:
                raise ValueError(f"File {proc} cannot be matched on udfs!")

            for udf in udfs:
                # udf_schema = udf[2]
                udf_name = udf[3]

                self.mappings[udf_name] = Mapping(table=self, schema=self.schema, name=udf_name)

        return None


class Column:
    def __init__(self, table, name, datatype):
        self.table = table
        self.name = name
        self.datatype = (
            Repo.datatype_translate[datatype] if datatype in Repo.datatype_translate else datatype
        )

    def __str__(self):
        return f"{self.table}.{self.name}"


class Mapping:
    def __init__(self, table, schema, name):
        self.table = table
        self.schema = schema
        self.name = name
        self.file = None
        self.definition = None
        self.transpiled_file = None

        self.__read_definition()

    def __str__(self):
        return f"{self.table.schema.db}.{self.schema}.{self.name}"

    def __read_definition(self):
        self.file = (
            os.path.join(
                self.table.schema.db.dir,
                self.schema.name,
                "Functions",
                self.name + ".sql"
            )
        )
        if not os.path.exists(self.file):
            raise ValueError(f"Could not find mapping {self.file}")

        with open(self.file, "r", encoding="utf-8-sig") as u:
            udf_definition = u.read()

        self.definition = re.sub(
            r"((.*)(CREATE)[ \t]+(FUNCTION)(.+)(\n)+(RETURNS TABLE)\n+(AS)\n+(RETURN)\n+)|((GO\n)*)|(EXECUTE)(.*)",
            "",
            udf_definition,
        )

        self.schema.db.repo.mappings.append(self)
