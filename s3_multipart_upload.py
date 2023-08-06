"""
Upload a video using presigned url + multi-part upload 
for video upload performance improvement.
"""
import requests
import boto3
import botocore
import base64
import hashlib

s3 = boto3.client("s3")

input_dir = "inputs/"
video_name = "raj-archery.mp4"
bucket = "human-action-recognition-bucket"
key = input_dir + video_name

response = s3.create_multipart_upload(Bucket=bucket, Key=key)

upload_id = response["UploadId"]


parts = []
i = 1
max_size = 6 * 1024 * 1024
local_video_path = "./input/" + video_name
with open(local_video_path, mode="rb") as f:
    chunk = f.read(max_size)
    while len(chunk) > 0:
        # md = hashlib.md5(chunk).digest()
        # contents_md5 = base64.b64encode(md).decode("utf-8")
        try:
            signed_url = s3.generate_presigned_url(
                ClientMethod="upload_part",
                Params={
                    "Bucket": bucket,
                    "Key": key,
                    "UploadId": upload_id,
                    "PartNumber": i,
                },
                ExpiresIn=1000,
            )
        except botocore.exceptions.ClientError as error:
            print(error)
            raise
        print(signed_url)
        # data = {"Content-Length": len(contents_md5), "Content-MD5": contents_md5}
        res = requests.put(signed_url, data=chunk)
        print(res.text)
        etag = res.headers["ETag"]
        parts.append({"ETag": etag, "PartNumber": i})
        chunk = f.read(max_size)
        print(i)
        i += 1
print(parts)
res = s3.complete_multipart_upload(
    Bucket=bucket, Key=key, MultipartUpload={"Parts": parts}, UploadId=upload_id
)
