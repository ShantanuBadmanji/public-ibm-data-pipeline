from time import sleep
from ibm_dataengine import run_sql, get_jobid_status

MAX_ATTEMPTS = 3


def transform_cos_file(
    sql_file_name: str,
    sql_query_params: "dict[str, str]" = dict(),
    sleep_time=30,
):
    for attempt in range(MAX_ATTEMPTS):
        job_info = run_sql(sql_file_name, sql_query_params)

        if (job_info) and len(job_info["job_id"]) > 0:
            break
        elif attempt < MAX_ATTEMPTS - 1:
            print("error in recieving job-id")
            sleep(2 * 60)
        else:
            print("ERROR: Exceeded the max attempts to run sql")
            exit(1)

    while True:
        job_status = get_jobid_status(jobid=job_info["job_id"])
        if job_status in ["queued", "running", "stopping"]:
            sleep(sleep_time)
        else:
            break

    if job_status not in ["completed"]:
        print(f"sql-job is not completed. STATUS: {job_status}")
        exit(1)


if __name__ == "__main__":
    transform_cos_file(
        sql_file_name="clean_transform_source_data.sql",
        sql_query_params={
            "<source-bucket-name>": "shantanu-grp-source-files",
            "<stage-bucket-name>": "grp-stage",
            "<path-to-source-file>": "closed.csv",
        },
    )
