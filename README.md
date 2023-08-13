

# Improve performance of video upload to S3 using S3 presigned url with multipart upload.

# How it works

Multipart upload of an object(file, audio, video, etc) to AWS S3 bucket.
Divide an input vidoe file into multiple parts and generate presigned url for each part.
Upload each part of the video with presigned url to S3 bucket.
