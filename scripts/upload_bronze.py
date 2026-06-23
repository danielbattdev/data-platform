from pathlib import Path
import boto3

MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin123"

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

bucket = "bronze"

source_dir = Path(
    "/home/daniel/data-platform/storage/bronze/olist"
)

for file in source_dir.glob("*.parquet"):
    object_name = f"olist/{file.name}"

    print(f"Enviando {file.name}...")

    s3.upload_file(
        str(file),
        bucket,
        object_name
    )

    print(f"✓ {object_name}")

print("\nUpload concluído!")
