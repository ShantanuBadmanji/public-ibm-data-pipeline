import ibm_db
from logger import table_info_logger
import os
from dotenv import load_dotenv

load_dotenv()

DB2_ACCESS_KEY_ID = os.getenv("DB2_ACCESS_KEY_ID")
DB2_SECRET_ACCESS_KEY = os.getenv("DB2_SECRET_ACCESS_KEY")
DB2_USER = os.getenv("DB2_USER")
DB2_PASS = os.getenv("DB2_PASS")


def load_to_db2(bucket_name, file_name, table_name):
    ibm_db_conn = ibm_db.connect("DYSSPO_DB2", DB2_USER, DB2_PASS)
    print("connected to db2")
    # conn = ibm_db_dbi.Connection(ibm_db_conn)

    load_from_cos_source_bucket_using_cli = f"LOAD FROM 'S3::s3.us-south.cloud-object-storage.appdomain.cloud::{DB2_ACCESS_KEY_ID}::{DB2_SECRET_ACCESS_KEY}::{bucket_name}::{file_name}' OF DEL REPLACE INTO {table_name}"
    try:
        statement = ibm_db.callproc(
            ibm_db_conn,
            "SYSPROC.ADMIN_CMD",
            (load_from_cos_source_bucket_using_cli,),
        )
        print(statement)

        stat2 = ibm_db.exec_immediate(
            ibm_db_conn,
            f"select * from {table_name} order by ID_RSSD limit 2",
        )

        while True:
            row = ibm_db.fetch_tuple(stat2)
            if row:
                print(row)
                print()
            else:
                break

        stat2 = ibm_db.exec_immediate(
            ibm_db_conn,
            f"select count(*) from {table_name}",
        )

        row = ibm_db.fetch_tuple(stat2)
        if row:
            table_info_logger(table_name, row[0])

        return statement

    except Exception as e:
        print(f"Error in loading data into {table_name}: {str(e)}")


if __name__ == "__main__":
    load_to_db2(
        bucket_name="grp-stage",
        file_name="clean_data.csv",
        table_name="CLOSED",
    )
