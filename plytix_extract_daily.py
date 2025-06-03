"""Extract Plytix product data.

This Glue job fetches product data from Plytix CSV feed, standardizes the fields using
Transmog, and saves it in both JSON (gzipped) and Parquet formats to S3 buckets.

The job performs the following main tasks:
1. Fetches product data from Plytix CSV feed
2. Uses Transmog to process and standardize the data
3. Saves data in both JSON and Parquet formats with appropriate compression
"""

# Standard library imports
import gzip
import io
import os
import shutil
import sys
import tempfile
from datetime import datetime, timezone

# Third party imports
import boto3
import requests
from boto3.session import Session

import transmog as tm

# Configuration constants
AWS_REGION = "us-east-1"
JSON_BUCKET = "dbc-incremental-data"
PARQUET_BUCKET = "dbc-plytix"

# S3 path templates for data storage
PRODUCT_JSON_PATH = (
    "plytix/products/{timestamp:%Y/%m/%d/%H}/plytix-{timestamp:%M}.json.gz"
)
PRODUCT_PARQUET_PATH = (
    "products/{timestamp:%Y/%m/%d/%H}/plytix-{timestamp:%M}.snappy.parquet"
)

# SSM parameter paths
SSM_PLYTIX_FEED_URL = "/plytix/feed_url"


def get_ssm_parameter(value: str, region: str, decrypt: bool = False) -> str:
    """Retrieve a parameter value from AWS Systems Manager Parameter Store.

    Args:
        value: Parameter name to retrieve.
        region: AWS region where the parameter is stored.
        decrypt: Whether to decrypt the parameter value.

    Returns:
        str: The parameter value or empty string if parameter is invalid.

    Raises:
        boto3.exceptions.Boto3Error: If there's an error accessing SSM.
    """
    ssm_client = boto3.client("ssm", region_name=region)
    data = ssm_client.get_parameters(Names=[value], WithDecryption=decrypt)
    return (
        data["Parameters"][0].get("Value", "") if not data["InvalidParameters"] else ""
    )


def fetch_csv_data(url: str) -> bytes:
    """Fetch CSV data from the specified URL.

    Args:
        url: URL to fetch CSV data from.

    Returns:
        bytes: Raw CSV data.

    Raises:
        requests.exceptions.RequestException: If the request fails.
    """
    try:
        print("Fetching CSV data from Plytix feed")

        # Long timeout for large files
        response = requests.get(url, timeout=900)
        response.raise_for_status()

        content_size = len(response.content)
        print(f"Download complete: {content_size / (1024 * 1024):.1f} MB")

        return response.content
    except Exception as e:
        print(f"Failed to fetch CSV data: {str(e)}")
        raise


def process_with_transmog(csv_data: bytes) -> tm.ProcessingResult:
    """Process CSV data using Transmog.

    Uses the csv_optimized configuration to handle all values as strings
    and disables type inference to ensure consistent results. Forces native
    CSV implementation to avoid PyArrow performance issues with large files.

    Args:
        csv_data: Raw CSV data as bytes.

    Returns:
        tm.ProcessingResult: Processed data result object.

    Raises:
        Exception: If processing fails.
    """
    try:
        print(f"Processing CSV data: {len(csv_data) / (1024 * 1024):.1f} MB")

        # String-optimized configuration with metadata preserved and string casting
        # CRITICAL: Force native CSV reader for performance with large files
        config = (
            tm.TransmogConfig.csv_optimized()
            .with_metadata()
            .with_processing(cast_to_string=True)
        )

        # Override CSV reader selection to force native implementation
        # This eliminates the PyArrow performance bottleneck for large files
        os.environ["TRANSMOG_FORCE_NATIVE_CSV"] = "true"

        processor = tm.Processor(config=config)

        # Store CSV data in temporary file
        with tempfile.NamedTemporaryFile(
            mode="wb",
            delete=False,
            suffix=".csv",
            buffering=8 * 1024 * 1024,  # 8MB buffer
        ) as temp_file:
            temp_file.write(csv_data)
            temp_file_path = temp_file.name

        try:
            print("Starting Transmog processing with native CSV reader...")
            start_time = datetime.now()

            result = processor.process_csv(
                file_path=temp_file_path,
                entity_name="product",
                has_header=True,
                sanitize_column_names=True,
                infer_types=False,  # Prevent type conversion
                encoding="utf-8",
                null_values=[],  # Prevent null conversions
                chunk_size=5000,  # Larger chunks for performance
            )
            processing_time = (datetime.now() - start_time).total_seconds()
            record_count = len(result.get_main_table())
            print(f"Processed {record_count} products in {processing_time:.1f} seconds")

            return result
        finally:
            # Clean up temporary file
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
            # Clean up environment override
            os.environ.pop("TRANSMOG_FORCE_NATIVE_CSV", None)

    except Exception as e:
        print(f"Failed to process data with Transmog: {str(e)}")
        raise


def prepare_json_output(processing_result: tm.ProcessingResult) -> bytes:
    """Prepare processing result as gzipped JSON.

    Args:
        processing_result: Transmog processing result.

    Returns:
        bytes: Gzipped JSON data.

    Raises:
        Exception: If JSON conversion or compression fails.
    """
    try:
        print("Preparing JSON output...")
        json_string = processing_result.to_json()
        compressed_data = gzip.compress(json_string.encode("utf-8"))
        print(
            f"JSON output: {len(compressed_data) / (1024 * 1024):.1f} MB (compressed)"
        )
        return compressed_data
    except Exception as e:
        print(f"Failed to prepare JSON output: {str(e)}")
        raise


def prepare_parquet_output(processing_result: tm.ProcessingResult) -> bytes:
    """Prepare processing result as Parquet with Snappy compression.

    Args:
        processing_result: Transmog processing result.

    Returns:
        bytes: Compressed Parquet data.

    Raises:
        Exception: If Parquet conversion fails.
    """
    try:
        print("Preparing Parquet output...")
        temp_dir = tempfile.mkdtemp()

        try:
            # Write with Snappy compression
            output_files = processing_result.write_all_parquet(
                base_path=temp_dir, compression="SNAPPY"
            )

            main_table_path = output_files.get("main")
            if not main_table_path or not os.path.exists(main_table_path):
                raise ValueError("Main table Parquet file not found")

            with open(main_table_path, "rb") as f:
                parquet_data = f.read()

            print(f"Parquet output: {len(parquet_data) / (1024 * 1024):.1f} MB")
            return parquet_data
        finally:
            # Clean up temporary directory
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
    except Exception as e:
        print(f"Failed to prepare Parquet output: {str(e)}")
        raise


def upload_to_s3(data: bytes, bucket: str, key: str, s3_client: boto3.client) -> bool:
    """Upload data to S3.

    Args:
        data: Bytes to upload.
        bucket: Target S3 bucket.
        key: Target S3 key.
        s3_client: Initialized boto3 S3 client.

    Returns:
        bool: True if upload successful, False otherwise.
    """
    data_size = len(data) if data else 0
    file_type = "parquet" if key.endswith(".parquet") else "json"

    print(f"Uploading {file_type} file: {data_size / (1024 * 1024):.1f} MB")

    try:
        # Handle Parquet files
        if file_type == "parquet":
            with tempfile.NamedTemporaryFile(
                delete=False, suffix=".parquet"
            ) as temp_file:
                temp_file.write(data)
                temp_file_path = temp_file.name

            try:
                with open(temp_file_path, "rb") as f:
                    # Multipart upload configuration
                    config = boto3.s3.transfer.TransferConfig(
                        multipart_threshold=8 * 1024 * 1024,  # 8MB
                        max_concurrency=10,
                        multipart_chunksize=8 * 1024 * 1024,  # 8MB per part
                        use_threads=True,
                    )
                    s3_client.upload_fileobj(f, Bucket=bucket, Key=key, Config=config)
                print("Parquet upload complete")
                return True
            finally:
                # Clean up temporary file
                if os.path.exists(temp_file_path):
                    os.remove(temp_file_path)
        else:
            # Handle JSON files
            config = boto3.s3.transfer.TransferConfig(
                multipart_threshold=8 * 1024 * 1024,  # 8MB
                max_concurrency=10,
                multipart_chunksize=8 * 1024 * 1024,  # 8MB per part
                use_threads=True,
            )

            file_obj = io.BytesIO(data)
            s3_client.upload_fileobj(file_obj, Bucket=bucket, Key=key, Config=config)
            print("JSON upload complete")
            return True
    except Exception as error:
        print(f"Write operation failed for {file_type} file: {str(error)}")
        return False


def main() -> None:
    """Main execution function to fetch and process Plytix data.

    Coordinates the entire ETL process:
    1. Fetches CSV data from Plytix feed
    2. Processes it with Transmog
    3. Saves to both JSON and Parquet formats

    Raises:
        Exception: If any part of the process fails.
    """
    # Configure stdout/stderr for proper encoding
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

    # Initialize AWS clients and timestamp
    now = datetime.now(timezone.utc)
    session = Session()
    s3_client = session.client("s3")

    print("Retrieving Plytix feed URL from SSM")
    url = get_ssm_parameter(SSM_PLYTIX_FEED_URL, AWS_REGION)

    # Validate URL format
    if not url or not url.startswith("http"):
        print("Error: Invalid or missing URL from SSM parameter")
        raise ValueError("Invalid Plytix feed URL from SSM")

    try:
        # Execute ETL pipeline
        csv_data = fetch_csv_data(url)
        processing_result = process_with_transmog(csv_data)

        # Skip if no products found
        if not processing_result.get_main_table():
            print("No products found - skipping all write operations")
            return

        # Generate and upload outputs
        json_output = prepare_json_output(processing_result)
        parquet_output = prepare_parquet_output(processing_result)

        json_key = PRODUCT_JSON_PATH.format(timestamp=now)
        parquet_key = PRODUCT_PARQUET_PATH.format(timestamp=now)

        json_result = upload_to_s3(json_output, JSON_BUCKET, json_key, s3_client)
        parquet_result = upload_to_s3(
            parquet_output, PARQUET_BUCKET, parquet_key, s3_client
        )

        successful_uploads = sum(
            1 for result in [json_result, parquet_result] if result
        )
        print(f"ETL process complete: {successful_uploads} of 2 uploads successful")

    except Exception as e:
        print(f"Processing failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
