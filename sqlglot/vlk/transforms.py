from __future__ import annotations

from sqlglot.vlk.helpers import norm_id
from sqlglot.optimizer.scope import Scope, traverse_scope
from sqlglot import expressions as exp
from sqlglot.optimizer.annotate_types import TypeAnnotator
from sqlglot.schema import MappingSchema
from sqlglot.helper import ensure_list


def transform(expression: exp.Expression, fun, *args) -> exp.Expression:  
    expression = fun(expression, *args)

    exp.replace_children(expression, lambda child: transform(child, fun, *args))
    return expression


def translate_add_to_dpipe(expression: exp.Expression) -> exp.Expression:
    if isinstance(expression, exp.Add):
        string_datatype = (
            exp.DataType.Type.NVARCHAR,
            exp.DataType.Type.VARCHAR,
            exp.DataType.Type.TEXT,
            exp.DataType.Type.CHAR,
            exp.DataType.Type.NCHAR,
        )
        date_datatype = (
            exp.DataType.Type.DATETIME,
            exp.DataType.Type.DATE,
            exp.DataType.Type.TIMESTAMPLTZ,
            exp.DataType.Type.TIMESTAMP,
        )

        if expression.this.type in string_datatype or expression.expression.type in string_datatype:
            return exp.DPipe(this=expression.this, expression=expression.expression)
        elif expression.this.type in date_datatype and expression.expression.type in date_datatype:
            return exp.UnixToStr(this=exp.Add(this=exp.StrToUnix(this=expression.this), expression=exp.StrToUnix(this=expression.this)))

    return expression


def normalize_table_references(expression: exp.Expression) -> exp.Expression:
    if isinstance(expression, exp.Table):
        table = expression.this
        db = expression.args.get("db")
        catalog = expression.args.get("catalog")
        alias = (
            norm_id(expression.alias.this)
            if isinstance(expression.alias, exp.Identifier)
            else norm_id(expression.alias)
        )
        return exp.Table(
            this=exp.Identifier(this=norm_id(table.this), quoted=False),
            db=exp.Identifier(this=norm_id(db.this), quoted=False),
            catalog=exp.Identifier(this=norm_id(catalog.this), quoted=False),
            alias=alias,
        )
    return expression


def normalize_column_references(expression: exp.Expression) -> exp.Expression:
    # Table qualifier in column reference
    if isinstance(expression, (exp.Dot, exp.Column)):
        this = expression
        column_info = []
        while isinstance(this, (exp.Dot, exp.Column)):
            if isinstance(this, exp.Dot):
                identifier = this.expression
                if isinstance(identifier, exp.Identifier):
                    column_info.append(exp.Identifier(this=norm_id(identifier.name), quoted=False))
                elif isinstance(identifier, str):
                    column_info.append(exp.Identifier(this=norm_id(identifier), quoted=False))
            else:
                column_identifier = this.this
                table_identifier = this.table

                if isinstance(column_identifier, exp.Identifier):
                    column_info.append(
                        exp.Identifier(this=norm_id(column_identifier.name), quoted=False)
                    )
                elif isinstance(column_identifier, str):
                    column_info.append(exp.Identifier(this=norm_id(column_identifier), quoted=False))

                if isinstance(table_identifier, exp.Identifier):
                    column_info.append(
                        exp.Identifier(this=norm_id(table_identifier.name), quoted=False)
                    )
                elif isinstance(table_identifier, str):
                    column_info.append(exp.Identifier(this=norm_id(table_identifier), quoted=False))

            if isinstance(this, exp.Column) and len(column_info) >= 2:
                return exp.Column(this=column_info[0], table=column_info[1])

            this = this.this

    return expression


def normalize_table_aliases(expression: exp.Expression) -> exp.Expression:
    if isinstance(expression, exp.TableAlias):
        table_identifier = expression.this
        if isinstance(table_identifier, exp.Identifier):
            return exp.TableAlias(
                this=exp.Identifier(
                    this=norm_id(table_identifier.this), quoted=False
                ),
                columns=expression.columns,
            )

    return expression


def normalize_column_aliases(expression: exp.Expression) -> exp.Expression:
    if isinstance(expression, exp.Alias):
        if expression.find_ancestor(exp.Subquery):

            column_identifier = expression.alias
            if isinstance(column_identifier, exp.Identifier):
                return exp.Alias(
                    this=expression.this,
                    alias=norm_id(column_identifier.this),
                    quoted=False,
                )
            if isinstance(column_identifier, str):
                return exp.Alias(
                    this=expression.this, alias=norm_id(column_identifier), quoted=False
                )

    return expression


def annotate_udtf(expression: exp.Expression) -> exp.Expression:
    if isinstance(expression, TypeAnnotator.TRAVERSABLES):
        udtf_column_schema = {
            "reference": {
                "trn": {
                    "getperiod": {
                        "cd_periode": "VARCHAR",
                    },
                }
            },
        }
        udtf_schema = MappingSchema(schema=udtf_column_schema)

        if isinstance(expression, TypeAnnotator.TRAVERSABLES):
            for scope in traverse_scope(expression):
                for name, source in scope.sources.items():
                    columns = []
                    if isinstance(source, Scope) and isinstance(source.expression, exp.Lateral):
                        lateral = source.expression
                        this = lateral.this
                        if isinstance(this, exp.Dot):
                            function = this.expression
                            this = this.this
                            if isinstance(this, exp.Dot):
                                schema = this.expression
                                db = this.this
                                db = db.this.lower()
                                schema = schema.this.lower()
                                function = function.this.lower()
                                _table = exp.Table(catalog=db, db=schema, this=function)

                                for column in scope.source_columns(name):
                                    try:
                                        column_identifier = column.this
                                        column_name = column_identifier.this.lower()
                                        column.type = udtf_schema.get_column_type(
                                            _table, column_name
                                        )
                                        columns.append(exp.Identifier(this=column_name))
                                    except Exception as e:
                                        raise ValueError(
                                            f"No schema available for column {column_name} at table {_table}"
                                        )

                                # Set column information on UDTF
                                alias = lateral.args["alias"]
                                alias.args["columns"] = list(
                                    exp.Identifier(this=column)
                                    for column in udtf_column_schema.get(db)
                                    .get(schema)
                                    .get(function)
                                )

    return expression


def translate_db_and_table(expression: exp.Expression, db_prefix) -> exp.Expression:
    if isinstance(expression, exp.Table) and db_prefix:
        table = expression.this
        db = expression.args.get("db")
        catalog = expression.args.get("catalog")

        new_table = f"{db.this.lower()}_{table.this.lower()}"
        new_db = f"{db_prefix}{catalog.this.lower()}"
        new_catalog = None
        return exp.Table(
            this=exp.Identifier(this=new_table, quoted=False),
            db=exp.Identifier(this=new_db, quoted=False),
            catalog=exp.Identifier(this=new_catalog, quoted=False),
            alias=expression.alias,
        )
    return expression


def translate_udtf(expression: exp.Expression, db_prefix) -> exp.Expression:
    if isinstance(expression, exp.Dot):
        function = expression.expression
        dot = expression.this
        if isinstance(dot, exp.Dot):
            function_name = f"{dot.expression}_{function.this}".lower()
            db = dot.this
            db = f"{db_prefix}{db.this}".lower()
            
            return exp.Dot(
                this=db,
                expression=exp.Anonymous(
                    this=function_name,
                    expressions=function.expressions,
                ),
            )

    return expression


def values_to_union(expression: exp.Expression) -> exp.Expression:
    if isinstance(expression, exp.Values):

        # Check if any references to columns is applicable
        if len(set(expression.find_all(exp.Column))) > 0:
            new_expr = None
            for tpl in expression.expressions:
                if new_expr is None:
                    new_expr = exp.Select(expressions=tpl.expressions)
                else:
                    new_expr = exp.Union(this=new_expr, expression=exp.Select(expressions=tpl.expressions))
            return new_expr

    return expression


def subquery_to_join(expression: exp.Expression) -> exp.Expression:
    if isinstance(expression, exp.Select):
        joins = []
        table_aliases = []

        # Find subqueries in select context
        for expr in expression.expressions:
            for subquery in expr.find_all((exp.Subquery, exp.Anonymous)):

                # Set new alias for table
                alias = subquery.alias.lower()
                if not alias:
                    alias = subquery.find_ancestor(exp.Alias).alias
                table_alias = f"trn_{alias.lower()}"

                # Handling custom functions
                if isinstance(subquery, exp.Anonymous):

                    def build_predicate(on, predicate, expr1, expr2, expr3=None):
                        if predicate == exp.Between:
                            pred = predicate(this=expr1, low=expr2, high=expr3)
                        else:
                            pred = predicate(this=expr1, expression=expr2)
                        
                        if on is None:
                            on = pred
                        else:
                            on = exp.And(this=on, expression=pred)

                        return on

                    if subquery.this.lower() == "trn_gettranslation":

                        # Replace scalar function with an actual column reference
                        is_verplicht = subquery.expressions[5]
                        replace_expr = exp.Column(this="cd_doelformaat", table=table_alias)
                        if is_verplicht.this == "1":
                            replace_expr = exp.Coalesce(this=replace_expr, expression=exp.Literal.string('NVT'))

                        # Also replace dot parents if available
                        db_qualifier = subquery.find_ancestor(exp.Dot)
                        this = db_qualifier if db_qualifier is not None else subquery
                        parent = this.parent
                        parent.set(this.arg_key, replace_expr)

                        parameter_labels = {
                            0: "cd_eigenaar",
                            1: "nm_tabel",
                            2: "nm_kolom",
                            3: "cd_bronformaat",
                            4: "meta_dt_snapshot",
                        }

                        if table_alias not in table_aliases:
                            on = None
                            for idx, parameter in enumerate(subquery.expressions[:5]):
                                column_identifier = parameter_labels[idx]
                                column = exp.Column(this=column_identifier, table=table_alias)

                                if column_identifier == "meta_dt_snapshot":
                                    on = build_predicate(
                                        on=on,
                                        predicate=exp.Between,
                                        expr1=parameter,
                                        expr2=exp.Column(this="meta_dt_valid_from", table=table_alias),
                                        expr3=exp.Column(this="meta_dt_valid_to", table=table_alias),
                                    )
                                else:
                                    on = build_predicate(on=on, predicate=exp.EQ, expr1=column, expr2=parameter)

                            table = exp.Table(this="trn_translatie", db="vl_dwh_reference", alias=exp.TableAlias(this=table_alias))
                            joins.append(exp.Join(this=table, side="LEFT", on=on))

                            table_aliases.append(table_alias)

                # Handling correlated subqueries
                elif isinstance(subquery, exp.Subquery):

                    # Set subquery select and its single expression
                    select = subquery.this.copy()
                    sel_expr = select.expressions[0].copy()
                    subquery_new = subquery.copy()

                    # Replace subquery with an actual column reference
                    replace_expr = sel_expr.this if isinstance(sel_expr, exp.Alias) else sel_expr
                    for column in replace_expr.find_all(exp.Column):
                        column.set("table", table_alias)

                    parent = subquery.parent
                    parent.set(subquery.arg_key, replace_expr)

                    # Only add the join to such a table once
                    if table_alias not in table_aliases:

                        # Determine whether it is a correlated subquery
                        scopes = traverse_scope(select)
                        curr_scope = scopes[0]
                        correlated_columns = set(column for column in curr_scope.columns if column.table not in curr_scope.sources)
                        correlated = True if len(correlated_columns) > 0 else False
                        
                        # Determine whether there are any aggregations taking place
                        aggregations = True if len(set(sel_expr.find_all(exp.AggFunc))) > 0 else False

                        # Determine whether there is a limit used
                        limit = True if select.args.get("limit") else False

                        # Determine whether a group by or distinct is used
                        grouped = select.args.get("group") is not None or select.args.get("distinct") is not None

                        # Determine whether it is a plain join on a single table
                        plain_join = select.args.get("joins") is None and isinstance(select.args.get("from").expressions[0], exp.Table)
                        
                        # CROSS JOIN in case of an uncorrelated subquery
                        if not correlated:
                            subquery_new.set("alias", exp.TableAlias(this=table_alias))
                            expression = expression.join(expression=subquery_new, join_type="LEFT OUTER", on=exp.EQ(this=exp.Literal.number(1), expression=exp.Literal.number(1)))

                        # LEFT JOIN in case of a correlated subquery without any special characteristics
                        elif correlated and plain_join and not (aggregations or limit or grouped):
                            select = subquery_new.this
                            table = select.args.get("from").expressions[0]
                            table.set("alias", exp.TableAlias(this=table_alias))
                            where = select.args.get("where").this

                            for column in (column for column in where.find_all(exp.Column) if column.table in curr_scope.sources):
                                column.set("table", table_alias)

                            joins.append(exp.Join(this=table, side="LEFT", on=where))

                        # LEFT LATERAL JOIN in case of a correlated subquery with any characteristics of the following:
                        # - Aggregations
                        # - Grouping
                        # - Limit
                        # - Multiple tables/subqueries involved
                        else:
                            select = subquery_new.this
                            scopes = traverse_scope(select)
                            for scope in scopes:
                                move_predicates = []
                                for column in (column for column in scope.columns if column.table not in scope.sources):
                                    or_predicate = column.find_ancestor((exp.Or, exp.Where, exp.Having, exp.Join))
                                    if isinstance(or_predicate, exp.Or):
                                        raise ValueError(f"Or predicate found while transforming correlated subquery {scope}")
                                    
                                    join = column.find_ancestor(exp.Join)
                                    if join:
                                        predicate = column.find_ancestor(exp.Predicate)
                                        move_predicates.append(predicate)

                                # Set where predicates
                                predicates = select.args.get("where")
                                for predicate in set(move_predicates):
                                    if predicates is None:
                                        predicates = predicate
                                    else:
                                        predicates = exp.And(this=predicates, expression=predicate)

                                if predicates:
                                    select.set("where", predicates)

                                # Set join predicates
                                select = scope.expression
                                predicates = None
                                for join in select.args.get("joins"):
                                    on = join.args.get("on")
                                    for predicate in on.find_all(exp.Predicate):
                                        if predicate not in move_predicates:
                                            if predicates is None:
                                                predicates = predicate
                                            else:
                                                predicates = exp.And(this=predicates, expression=predicate)
                                    if predicates is None:
                                        predicates = exp.EQ(this=exp.Literal.number(1), expression=exp.Literal.number(1))
                                    join.set("on", predicates)

                            subquery_new.set("alias", exp.TableAlias(this=table_alias))
                            expression = expression.join(expression=exp.Lateral(this=subquery_new), join_type="LEFT")

                        # Add table alias to list to ensure no double table references are applied
                        table_aliases.append(table_alias)

        
        all_joins = ensure_list(expression.args.get("joins")) + ensure_list(joins)
        expression.set("joins", all_joins)

    return expression

