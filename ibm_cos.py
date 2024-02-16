import ibm_boto3
from ibm_botocore.client import Config, ClientError
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")
COS_INSTANCE_CRN = os.getenv("COS_INSTANCE_CRN")

# IBM COS credentials
cos_credentials = {
    "apikey": API_KEY,
    "iam_service_endpoint": "https://iam.cloud.ibm.com/identity/token",
    "cos_endpoint": "https://s3.us-south.cloud-object-storage.appdomain.cloud",
    "resource_instance_id": COS_INSTANCE_CRN,
}

# COS client configuration
cos_config = Config(signature_version="oauth", region_name="us-south")

# Create COS client
cos_client = ibm_boto3.client(
    "s3",
    ibm_api_key_id=cos_credentials["apikey"],
    ibm_service_instance_id=cos_credentials["resource_instance_id"],
    ibm_auth_endpoint=cos_credentials["iam_service_endpoint"],
    config=cos_config,
    endpoint_url=cos_credentials["cos_endpoint"],
)


def rename_file(
    source_bucket, source_object_key, destination_bucket, destination_object_key
):
    try:
        # Copy the object within the same bucket with a new object key
        copy_source = {"Bucket": source_bucket, "Key": source_object_key}
        cos_client.copy_object(
            Bucket=destination_bucket,
            CopySource=copy_source,
            Key=destination_object_key,
        )
        # cos_client.delete_object(Bucket=source_bucket, Key=source_object_key)

        print(
            f"Object copied from '{source_object_key}' to '{destination_object_key}'."
        )
    except ClientError as e:
        print(f"Error: {e}")


def get_bucket_keys(bucket_name, prefix_to_filter):
    try:
        response = cos_client.list_objects_v2(
            Bucket=bucket_name, Prefix=prefix_to_filter
        )

        if "Contents" in response:
            print(f"Contents of the '{bucket_name}' bucket:")
            obj_list = [obj["Key"] for obj in response["Contents"]]
            return obj_list
        else:
            print(f"The '{bucket_name}' bucket is empty.")
            return []
    except Exception as e:
        print(f"Error listing objects: {str(e)}")


def rename_file_with_prefix(
    source_bucket: str,
    prefix_to_filter: str,
    destination_bucket: str,
    destination_object_key: str,
    file_extension: str = ".csv",
):
    obj_list: list[str] = get_bucket_keys(
        bucket_name=source_bucket,
        prefix_to_filter=prefix_to_filter,
    )

    print(f"obj_list: {obj_list}")
    filename = [obj for obj in obj_list if (obj.endswith(file_extension))][0]

    print(f"filename: {filename}")

    rename_file(
        source_bucket=source_bucket,
        destination_bucket=destination_bucket,
        source_object_key=filename,
        destination_object_key=destination_object_key,
    )


if __name__ == "__main__":
    rename_file_with_prefix(
        source_bucket="grp-stage",
        prefix_to_filter="ACTIVE_STAGE/part",
        destination_bucket="grp-stage",
        destination_object_key="ACTIVE_STAGE.CSV",
    )
