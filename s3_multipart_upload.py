"""
Upload a video using presigned url + multi-part upload 
for video upload performance improvement.
"""
import argparse
import requests
import boto3
import botocore
import base64
import hashlib
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

    s3 = boto3.client("s3")

    video_name = args["video_name"]
    bucket = args["bucket"]
    key = video_name

    print("Create multipart uplaod")
    response = s3.create_multipart_upload(Bucket=bucket, Key=key)

    upload_id = response["UploadId"]

    parts = []
    i = 1
    # all chunk size has to be at least 5MB
    max_size = 6 * 1024 * 1024
    # TODO: max_size changes depend on the input file size
    print(f"chunk size: {max_size}")
    local_video_path = video_name
    with open(local_video_path, mode="rb") as f:
        chunk = f.read(max_size)
        while len(chunk) > 0:
            # md = hashlib.md5(chunk).digest()
            # contents_md5 = base64.b64encode(md).decode("utf-8")
            print("Generating presigned url for video name: {video_name}")
            try:
                presigned_url = s3.generate_presigned_url(
                    ClientMethod="upload_part",
                    Params={
                        "Bucket": bucket,
                        "Key": key,
                        "UploadId": upload_id,
                        "PartNumber": i,
                    },
                    ExpiresIn=1000,
                )
            except botocore.exceptions.ClientError as e:
                print("Error generating pre-signed URL:", str(e))
                sys.exit(1)
            print(f"Generated presigned url: {presigned_url}")
            # data = {"Content-Length": len(contents_md5), "Content-MD5": contents_md5}
            response = requests.put(presigned_url, data=chunk)
            print(f"test of response: {response.text}")
            etag = response.headers["ETag"]
            parts.append({"ETag": etag, "PartNumber": i})
            chunk = f.read(max_size)
            print(f"Etag: {etag} and PartNumber: {i}")
            i += 1
    print(f"parts: {parts}")

    print(
        f"Complete multipart upload with # of parts: {len(parts)} and upload id: {upload_id}"
    )
    res = s3.complete_multipart_upload(
        Bucket=bucket, Key=key, MultipartUpload={"Parts": parts}, UploadId=upload_id
    )
    print("Upload complete")
