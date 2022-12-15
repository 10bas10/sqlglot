from tqdm import tqdm
import pandas as pd
import sys, os, datetime
from databricks import sql
import sqlglot
from sqlglot import exp
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
    transform,
    values_to_union,
)
from sqlglot.vlk.generator import (
    format_parameter,
    hardcode_parameter,
)

class dbxConnect:
    def __init__(self):
        self.parameters = {}
        self.conn = sql.connect(
            server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
            http_path = os.getenv("DATABRICKS_HTTP_PATH"),
            access_token = os.getenv("DATABRICKS_TOKEN")
        )

    def parameterize_sql(self, sql):
        norm_sql = sql.format_map({par: par} for par in self.parameters.keys())
        return norm_sql.format_map(self.parameters)

    def execute(self, sql, as_view=False):
        cursor = self.conn.cursor()
        par_sql = self.parameterize_sql(sql)
        if as_view:
            par_sql = "CREATE OR REPLACE TEMPORARY VIEW udf_test\nAS\n" + par_sql
        cursor.execute(par_sql)
        result = cursor.fetchall()
        cursor.close()
        return result

    def close(self):
        self.conn.close()

    def set_parameter(self, parameter, value):
        if isinstance(value, str):
            par_value = f"'{value}'"
        else:
            par_value = value

        self.parameters[parameter] = par_value


class HiddenPrints:
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, "w")

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout


class batchTranspile:
    def __init__(self, root, scope, filter, view_metadata_csv, logdir=None, outputdir=None, db_prefix=None):
        self.root = root
        self.scope = scope
        self.filter = filter
        self.db_prefix = db_prefix
        self.repo = Repo(root=self.root, scope=self.scope, filter=self.filter, db_prefix=self.db_prefix)
        self.schema = self.repo.get_schema_dict(view_metadata_csv=view_metadata_csv)
        self.transpile_errors = None
        self.test_errors = None
        self.outputdir = outputdir
        self.logdir = logdir

        if not self.logdir:
            self.logdir = os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                "log"
            )
        if not self.outputdir:
            self.outputdir = os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                "output"
            )

    def flush_dir(self, dir):
        for f in os.listdir(dir):
            os.remove(os.path.join(dir, f))

    def transpile(self):
        self.transpile_errors = pd.DataFrame(columns=["database", "schema", "table", "udf", "errortype"])
        logdir = os.path.join(self.logdir, "transpile")
        self.flush_dir(logdir)
        self.flush_dir(self.outputdir)

        if not self.repo:
            raise ValueError("Repository not initialized yet!")

        for mapping in tqdm(iterable=self.repo.mappings, desc="Transpiling"):
            expr = None

            try:
                with HiddenPrints():

                    expr = sqlglot.parse_one(mapping.definition, read="tsql")

                    # Get the fully qualified table reference
                    expr = qualify_tables(expression=expr, catalog=mapping.table.schema.db.name)

                    # Normalize table references
                    expr = transform(expression=expr, fun=normalize_table_references)

                    # Normalize column references
                    expr = transform(expression=expr, fun=normalize_column_references)

                    # Normalize table aliases
                    expr = transform(expression=expr, fun=normalize_table_aliases)

                    # Normalize column aliases
                    expr = transform(expression=expr, fun=normalize_column_aliases)

                    # # Annotate UDTF attributes with a datatype
                    expr = transform(expression=expr, fun=annotate_udtf)

                    # # Get the fully qualified column names
                    expr = qualify_columns(expression=expr, schema=self.schema, check_unknown_tables=False)

                    # Get the datatypes of columns
                    expr = annotate_types(expr, schema=self.schema)

                    # Translate text concat functions
                    expr = transform(expression=expr, fun=translate_add_to_dpipe)

                    # Change database and table references according to hive tables
                    expr = transform(expr, translate_db_and_table, self.db_prefix)

                    # Change database and function references according to hive structure
                    expr = transform(expr, translate_udtf, self.db_prefix)

                    # Transform correlated subqueries to a regular left join, lateral or cross join
                    expr = transform(expr, subquery_to_join)

                    # Transform correlated tuples to a UNION ALL
                    expr = transform(expr, values_to_union)

                    # # Normalize table references
                    # expr = expr.transform(normalize_table_references)

                    # # Normalize column references
                    # expr = expr.transform(normalize_column_references)

                    # # Normalize table aliases
                    # expr = expr.transform(normalize_table_aliases)

                    # # Normalize column aliases
                    # expr = expr.transform(normalize_column_aliases)

                    # # # Annotate UDTF attributes with a datatype
                    # expr = expr.transform(annotate_udtf)

                    # # # Get the fully qualified column names
                    # expr = qualify_columns(expression=expr, schema=self.schema, check_unknown_tables=False)

                    # # Get the datatypes of columns
                    # expr = annotate_types(expr, schema=self.schema)

                    # # Translate text concat functions
                    # expr = expr.transform(translate_add_to_dpipe)

                    # # Change database and table references according to hive tables
                    # expr = expr.transform(translate_db_and_table, self.db_prefix)

                    # # Change database and function references according to hive structure
                    # expr = expr.transform(translate_udtf, "vl_dwh_")

                    # # Transform correlated subqueries to a regular left join, lateral or cross join
                    # expr = expr.transform(subquery_to_join)

                    # Set VLK custom generator functions
                    dbx = sqlglot.Dialect.get_or_raise("databricks")()
                    dbx_tfm = dbx.Generator.TRANSFORMS

                    #dbx_tfm[exp.Parameter] = format_parameter
                    dbx_tfm[exp.Parameter] = hardcode_parameter
                    
                    # Transpile expression
                    transpiled = dbx.generate(expr, pretty=True)
                    outputfile = os.path.join(
                        self.outputdir,
                        f"{mapping.table.schema.db.name}_{mapping.table.schema.name}_{mapping.table.name}_{mapping.name}.sql",
                    )
                    mapping.transpiled_file = os.path.abspath(outputfile)
                    with open(mapping.transpiled_file, "w", encoding="utf-8") as f:
                        f.write(transpiled)

            except Exception as errormsg:
                e_splitted = str(errormsg).splitlines()
                first_line = e_splitted[0].split(". Line")[0]
                error = {
                    "database": mapping.table.schema.db.name,
                    "schema": mapping.table.schema.name,
                    "table": mapping.table.name,
                    "udf": mapping.name,
                    "errortype": first_line,
                }
                self.transpile_errors = pd.concat(
                    [pd.DataFrame(error, index=[0]), self.transpile_errors], axis=0, ignore_index=True
                )

                logfile = os.path.join(
                    logdir,
                    f"{mapping.table.schema.db.name}_{mapping.table.schema.name}_{mapping.table.name}_{mapping.name}.log",
                )
                with open(logfile, "w", encoding="utf-8") as f:
                    f.write("Input:\n")
                    f.write(f"{mapping.definition}\n")
                    f.write("Parsing:\n")
                    f.write(str([expr]))
                    f.write("\n\nTranspiling:\n")
                    f.write(str(errormsg))
                # raise errormsg


    def create_source_tables(self, drop=False):
        conn = dbxConnect()
        
        repo = self.repo
        hive_tables = []
        for db, schemas in self.schema.items():
            if db not in self.repo.databases:
                continue
            for schema, tables in schemas.items():
                for table, columns in tables.items():
                    hive_tables.append({"database": db, "schema": schema, "table": table, "columns": columns})

        for table in tqdm(iterable=hive_tables, desc="Creating hive tables"):
            hive_db = f"{repo.db_prefix}{table['database']}"
            hive_tbl = f"{table['schema']}_{table['table']}"
            fq_tbl = f"`{hive_db}`.`{hive_tbl}`"

            columns_dt = (f"`{column}` {repo.datatype_translate[datatype.upper()] if datatype.upper() in repo.datatype_translate else datatype}" for column, datatype in table["columns"].items())
            column_ddl = ",\n".join(columns_dt)

            db_ddl = f"CREATE DATABASE IF NOT EXISTS `{hive_db}`;"
            conn.execute(db_ddl)

            if drop:
                drop_ddl = f"DROP TABLE IF EXISTS {fq_tbl};"
                conn.execute(drop_ddl)

            create_ddl = f"CREATE TABLE IF NOT EXISTS {fq_tbl} (\n{column_ddl});"
            conn.execute(create_ddl)

        conn.close()



    def test_mappings(self):
        conn = dbxConnect()

        conn.set_parameter("meta_nr_mapping", "1")
        conn.set_parameter("meta_dt_snapshot", datetime.datetime.strptime("1900-01-01", "%Y-%m-%d"))

        logdir = os.path.join(self.logdir, "parsing")
        self.flush_dir(logdir)

        self.test_errors = pd.DataFrame(columns=["database", "schema", "table", "udf", "errortype"])
        for mapping in tqdm(iterable=self.repo.mappings, desc="Testing"):
            if mapping.transpiled_file:
                with open(mapping.transpiled_file, 'r', encoding="utf-8") as f:
                    tsql = f.read()

                try:
                    conn.execute(sql=tsql, as_view=True)
                except Exception as errormsg:
                    e_splitted = str(errormsg).splitlines()
                    first_line = e_splitted[0].split("; Line")[0]
                    error = {
                        "database": mapping.table.schema.db.name,
                        "schema": mapping.table.schema.name,
                        "table": mapping.table.name,
                        "udf": mapping.name,
                        "errortype": first_line,
                    }
                    self.test_errors = pd.concat(
                        [pd.DataFrame(error, index=[0]), self.test_errors], axis=0, ignore_index=True
                    )
                    logfile = os.path.join(logdir, f"{mapping.table.schema.db.name}_{mapping.table.schema.name}_{mapping.table.name}_{mapping.name}.log",)
                    with open(logfile, "w", encoding="utf-8") as f:
                        f.write(str(errormsg))
                        f.write(conn.parameterize_sql(tsql))

        conn.close()
        

    def report(self):
        pd.set_option("display.max_rows", None)
        pd.set_option("display.max_colwidth", 100)

        succeeded = len(self.repo.mappings) - len(self.transpile_errors)
        print("Transpilation results")
        print(f"Succeeded:\t{succeeded}")
        print(f"Failed:\t\t{len(self.transpile_errors)}")
        print(f"Success ratio:\t{str(round(succeeded / len(self.repo.mappings) * 100, 1))}%")
        print()
        print("Transpile error information:")
        print(
            self.transpile_errors.groupby(["database", "errortype"])
            .agg(numErrors=("errortype", "count"))
            .reset_index()
            .set_index(["database", "errortype"])
        )
        print()

        parse_succes = succeeded - len(self.test_errors)
        print("Spark SQL parsing results")
        print(f"Succeeded:\t{parse_succes}")
        print(f"Failed:\t\t{len(self.test_errors)}")
        print(f"Success ratio:\t{str(round(parse_succes / len(self.repo.mappings) * 100, 1))}%")
        print()
        print("Spark SQL parse error information:")
        print(
            self.test_errors.groupby(["database", "errortype"])
            .agg(numErrors=("errortype", "count"))
            .reset_index()
            .set_index(["database", "errortype"])
        )