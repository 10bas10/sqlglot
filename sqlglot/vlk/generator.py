from sqlglot import expressions as exp

def format_parameter(self, e):
    variable = e.this
    var_name = variable.this.lower()
    return f"{{{{{var_name}}}}}"

def hardcode_parameter(self, e):
    variable = e.this
    var_name = variable.this.lower()

    if var_name == "meta_dt_snapshot":
        return "'9999-12-31'"
    if var_name == "meta_nr_mapping":
        return "'999'"
