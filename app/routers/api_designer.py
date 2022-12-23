from fastapi import APIRouter, File, UploadFile
import pandas as pd
from io import BytesIO

from utils.ddl_designer import update_df_column_names, generate_ddls, handle_ddls_snowflake

router = APIRouter()


@router.post("/ddl")
async def read_users(database_name, schema_name, table_name, separator=',', encoding='utf-8',
                     csv_file: UploadFile = File()):
    contents = csv_file.file.read()
    buffer = BytesIO(contents)
    data = pd.read_csv(buffer, delimiter=separator, encoding=encoding, nrows=10000)
    buffer.close()
    csv_file.file.close()
    data = update_df_column_names(data)
    ddl = generate_ddls(data, table_name, database_name, schema_name)
    ddl = handle_ddls_snowflake(ddl, data)
    return ddl
