import itertools

from sqlglot import alias, exp
from sqlglot.optimizer.scope import traverse_scope


def qualify_tables(expression, db=None, catalog=None):
    """
    Rewrite sqlglot AST to have fully qualified tables.

    Example:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one("SELECT 1 FROM tbl")
        >>> qualify_tables(expression, db="db").sql()
        'SELECT 1 FROM db.tbl AS tbl'

    Args:
        expression (sqlglot.Expression): expression to qualify
        db (str): Database name
        catalog (str): Catalog name
    Returns:
        sqlglot.Expression: qualified expression
    """
    sequence = itertools.count()

    for scope in traverse_scope(expression):
        for derived_table in scope.ctes + scope.derived_tables:
            if not derived_table.args.get("alias"):
                alias_ = f"_q_{next(sequence)}"
                derived_table.set("alias", exp.TableAlias(this=exp.to_identifier(alias_)))
                scope.rename_source(None, alias_)

        for source in scope.sources.values():
            if isinstance(source, exp.Table):
                identifier = isinstance(source.this, exp.Identifier)

                if identifier:
                    if not source.args.get("db"):
                        source.set("db", exp.to_identifier(db))
                    if not source.args.get("catalog"):
                        source.set("catalog", exp.to_identifier(catalog))

                if not source.alias:
                    source.replace(
                        alias(
                            source.copy(),
                            source.this if identifier else f"_q_{next(sequence)}",
                            table=True,
                        )
                    )

    return expression
