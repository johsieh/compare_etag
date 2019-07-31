import boto3
import hashlib
import os
from sys import argv

try:
    local_file = argv[1]
except:
    exit()

try:
    argv[2]
except:
    exit()
else:
    if argv[2].startswith('s3://'):
        s3_path = argv[2].split('/')
        bucket_name = s3_path[2]
        key_name = '/'.join(s3_path[3: ])
    else:
        exit()

s3 = boto3.client('s3')

head_object = s3.head_object(Bucket = bucket_name, Key = key_name)

s3_bytes = head_object['ContentLength']
s3_etag = head_object['ETag'].replace('"', '')
s3_etag_split = s3_etag.split('-')

local_bytes = os.stat(local_file).st_size
local_hash = hashlib.md5()

if s3_bytes == local_bytes:
    with open(local_file, 'rb') as file:
        if len(s3_etag_split) > 1:
            for part_number in range(1, int(s3_etag_split[1]) + 1):
                head_object_part = s3.head_object(Bucket = bucket_name, Key = key_name, PartNumber = part_number)
                local_part_file = file.read(head_object_part['ContentLength'])
                local_part_etag = hashlib.md5(local_part_file)
                local_hash.update(local_part_etag.digest())
            local_etag = '{}-{}'.format(local_hash.hexdigest(), part_number)
        else:
            local_hash.update(file.read())
            local_etag = local_hash.hexdigest()
else:
    local_etag = 'BYTES-DIFFERENCE-{}'.format(abs(s3_bytes - local_bytes))

print('\n')

if all( [ local_etag == s3_etag, local_bytes == s3_bytes ] ):
    print('* * * Files are the SAME * * *')
else:
    print('! ! ! Files are DIFFERENT ! ! !')

print('\n')
print('ETag: {}, Bytes: {} --> {}'.format(local_etag, local_bytes, local_file))
print('ETag: {}, Bytes: {} --> s3://{}/{}'.format(s3_etag, s3_bytes, bucket_name, key_name))
print('\n')
