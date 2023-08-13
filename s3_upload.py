"""
Upload a video file to S3 by directly invoking "GeneratePresignURL" lambda function.
This lambda function is not publicly accessible. 
"""
import argparse
import requests
import boto3
import botocore
import sys


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--video_name",
        dest="video_name",
        type=str,
        help="input video name",
    )
    parser.add_argument(
        "--bucket",
        dest="bucket",
        type=str,
        help="bucket name",
    )
    args = vars(parser.parse_args())

    print(f"Input video name: {args['video_name']}")
    print(f"Bucket name: {args['bucket']}")

    # Create an S3 client
    s3 = boto3.client("s3")

    # Specify the S3 bucket and object key
    video_name = args["video_name"]
    bucket_name = args["bucket"]
    object_key = video_name

    # Generate a pre-signed URL for uploading the object
    try:
        payload = s3.generate_presigned_post(
            Bucket=bucket_name,
            Key=object_key,
            ExpiresIn=1000,  # Expiration time in seconds
        )
        print(f"Pre-signed URL generated successfully. payload: {payload}")
    except botocore.exceptions.ClientError as e:
        print("Error generating pre-signed URL:", str(e))
        sys.exit(1)

    local_video_path = video_name
    input_videos = {"file": open(local_video_path, "rb")}
    print(
        f"Uploading a video {video_name} with presigned url {payload['url']} and fields {payload['fields']}"
    )
    r = requests.post(payload["url"], data=payload["fields"], files=input_videos)

    print(f"status code of response: {r.status_code}")
    print(f"test of response: {r.text}")
    print("Upload complete")
