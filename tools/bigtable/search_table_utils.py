def concat(fields, prefix="", separator="\"\""):
    '''
    Produces output like the following for an array of fields:
    CONCAT(
        IFNULL(location, ""), "",
        IFNULL(client_region_code, ""), "",
        IFNULL(client_country_code, ""), "",
        IFNULL(client_continent_code, "")
    )
    '''
    if len(fields) == 0:
        return ""
    output = "CONCAT("
    rows = []
    for field in fields:
        if len(prefix) == 0:
            rows.append("IFNULL({0}, {1}), \"\"\n".format(field, separator))
        else:
            rows.append("IFNULL({0}.{1}, {2}), \"\"\n".format(prefix, field, separator))
    output += ",".join(rows)
    output += ")"
    return output

def lower(query):
    '''
    Produces output like the following for a string:
    LOWER(<content>)
    '''
    if len(query) > 0:
        return "LOWER({0})".format(query)
    else:
        return ""

def replace(query, what_str, with_str):
    '''
    Produces output like the following:
    REPLACE(<query>, "what", "with")
    '''
    if len(query) > 0:
        return "REPLACE({0}, \"{1}\", \"{2}\")".format(query, what_str, with_str)
    else:
        return ""


def list_fields(fields, prefixes=[""], convert_to_sql=False):
    '''
    Produces a comma + newline separated list of field names like so:
    client_city,
    client_region,
    client_country
    '''
    output = []
    for prefix in prefixes:
        for field in fields:
            name = field_name(field)
            sql = name
            if(convert_to_sql):
                sql = field_sql(field)
            output.append(create_field_query(name, sql, prefix))


    return ", \n".join(output)

def create_field_query(name, sql, prefix = ""):
    '''
    outputs prefixed name if prefix is given and length > 0
    if sql is present and not equal to name, provide "sql AS prefix_name"
    '''
    out = ""
    if len(prefix) > 0:
        out = "{0}_{1}".format(prefix, name)
    else:
        out = "{0}".format(name)
    if (name == sql):
        return out
    else:
        return "{0} AS {1}".format(sql, name)


def field_sql(field):
    '''
    Pull out sql attribute of field
    '''
    if isinstance(field, dict):
        if "sql" in field:
            return field["sql"]
        else:
            return field["name"]
    return field

def field_name(field):
    '''
    pull out name attribute of field
    '''
    if isinstance(field, dict):
        return field["name"]
    return field


def all_table_fields(fields, tablenames=["all"], prefix_both=False):
    '''
    Produces a list of field mappings separated by comma and new line:

    all.fieldname as fieldname,
    all.otherfieldname as otherfieldname
    '''
    output = []

    for tablename in tablenames:
        if prefix_both:
            template = "{0}.{{0}} as {0}_{{0}}".format(tablename)
        else:
            template = "{0}.{{0}} as {{0}}".format(tablename)

        for field in fields:
            name = field_name(field)
            output.append(template.format(name))

    return ",\n".join(output)

def join_on_fields(fields, subtable):
    template = "all.{0} = {1}.{0}"
    output = []
    for field in fields:
        name = field_name(field)
        output.append(template.format(name, subtable))

    return " and \n".join(output)


def timed_list_fields(fields, times):
    output = []
    for timename in times:
        for field in fields:
            name = field_name(field)
            output.append("{0}_{1}".format(timename, name))

    return ", \n".join(output)

def output_bin_string(field, bins):
    '''
    output like:
    concat(
        STRING(SUM(IF(IFNULL(download_speed_mbps, 0) < 4 ,1,0))), ",",
        STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 4 AND IFNULL(download_speed_mbps, 0) < 8, 1,0))), ",",
        STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 8 AND IFNULL(download_speed_mbps, 0) < 12, 1,0))), ",",
        ...
        STRING(SUM(IF(IFNULL(download_speed_mbps, 0) >= 100, 1,0)))
      ) as download_speed_mbps_bins,
    '''
    name = field_name(field)

    # 0 - field, 1 - start bin, 2 - end bin
    template = "STRING(SUM(IF(IFNULL({0}, 0) >= {1} AND IFNULL({0}, 0) < {2}, 1,0))), \",\","

    fieldstr = ""
    for idx, startbin in enumerate(bins):
        if idx + 1 < len(bins):
            fieldstr += template.format(name, startbin, bins[idx+1]) + "\n"
        else:
            fieldstr += "STRING(SUM(IF(IFNULL({0}, 0) >= {1}, 1,0)))".format(name, startbin) + "\n"

    return "concat({0}) as {1}_bins".format(fieldstr, name)

def output_bins(aggregate_table, field, output_prefix, bins):
    '''
    Produces output like so:

    '''
    line = "{0}.{1}_B{2}_{3} as {4}_{5}"
    final_line = "{0}.{1}_BGT{2} as {3}_{4}"
    output = []
    for idx, startbin in enumerate(bins):

        name = field_name(field)
        if bins[idx+1]:
            output.append(line.format(aggregate_table, name, startbin, bins[idx+1],
                output_prefix, name))
        else:
            output.append(final_line.format(aggregate_table, name, startbin,
                output_prefix, name))
    return ",\n".join(output)

def compute_bins(field, bins):
    '''
        first bin should be 0
    '''
    output = ""
    name = field_name(field)
    for idx, startbin in enumerate(bins):
        if idx == 0:
            output += "SUM(IF({0} < {1} ,1,0)) AS {0}_B{1}_{2},\n".format(
                name, startbin, bins[idx+1])
        elif idx == len(bins):
            output += "SUM(IF({0} >= {1}, 1,0)) AS {0}_BGT{1},".format(
                name, startbin)
        else:
            output += "SUM(IF({0} >= {1} AND {0} < {2}, 1,0)) AS {0}_B{1}_{2},".format(name, startbin, bins[idx+1])
    return output
