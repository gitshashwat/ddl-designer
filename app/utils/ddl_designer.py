import re

import pandas as pd


def get_all_date_columns(data):
    '''
    Get a list of all date columns from a csv file
    :param data:
    :return:
    '''
    date_collist = []
    for col in data.columns:
        try:
            if data[col].dtype == 'object':
                try:
                    data[col] = pd.to_datetime(data[col])
                    date_collist.append(col)
                except ValueError:
                    pass
        except Exception as exc:
            print(exc)
    return date_collist


def update_df_column_names(data):
    """
    Update column names in the dataframe after removing irrelevant characters, for snowflake ddl
    :param data:
    :return:
    """
    char_replacements_dict = \
        {"&": "", " ": "_", "+": "_", "-": "_", "(": "", ")": "", "$": "_", ".": "_"}
    for key in char_replacements_dict:
        data.columns = data.columns.str. \
            upper().str.replace(key, char_replacements_dict[key], regex=False)
    return data


def generate_ddls(data, table, database, schema):
    """
    Generate ddl for the dataframe
    :param data:
    :return:
    """
    ddl = f"""CREATE OR REPLACE TABLE {database}.{schema}.{table}("""
    try:
        datatype_list = list(data.dtypes)
        iterator_ = 0
        for col in data.columns:
            ddl += (col + " " + str(datatype_list[iterator_]) + ", ")
            iterator_ += 1

        return ddl[:-2] + ");"
    except Exception as exc:
        raise Exception(exc)


def handle_ddls_snowflake(ddl, data):
    """
    Handle ddls in snowflake with appropriate datatypes
    :param ddl:
    :return:
    """
    # handle basic datatypes
    datatype_replacements_dict = {"INT64": "NUMBER", "OBJECT": "VARCHAR", "FLOAT64": "FLOAT"}
    for key in datatype_replacements_dict:
        ddl = ddl.upper().replace(key, datatype_replacements_dict[key])

    # handle dates
    date_col_list = get_all_date_columns(data)
    for col in date_col_list:
        pattern = re.compile(r'(?<={})( )VARCHAR'.format(col))
        while pattern.search(ddl):
            ddl = pattern.sub(r'\1DATE', ddl)

    return ddl


if __name__ == '__main__':
    data = pd.read_csv('/Users/shashwatkumar/Desktop/22dec/app/tests/SampleCSVFile_2kb.csv', encoding='ISO-8859-1')
    data = update_df_column_names(data)
    table = 'dummy_table'
    db = 'dummy_db'
    schema = 'dummy_schema'
    ddl = (generate_ddls(data, table, db, schema))
    print(handle_ddls_snowflake(ddl, data))
