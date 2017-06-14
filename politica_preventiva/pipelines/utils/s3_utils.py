'''
This script performs efficient concatenation of files stored in S3. Given a
folder, output location, and optional suffix, all files with the given suffix
will be concatenated into one file stored in the output location.
Concatenation is performed within S3 when possible, falling back to local
operations when necessary.
Run `python combineS3Files.py -h` for more info.
'''

import boto3
import os
import threading
import argparse
import logging
import pdb

# Script expects everything to happen in one bucket
BUCKET = "" # set by command line args
# S3 multi-part upload parts must be larger than 5mb
MIN_S3_SIZE = 6000000
# Setup logger to display timestamp
logging.basicConfig(format='%(asctime)s => %(message)s')

def run_concatenation(bucket, folder_to_concatenate, result_filepath, file_suffix, max_filesize=999999999):
    bucket_split_path = [x for x in bucket.split('/') if x and x != 's3:']
    if len(bucket_split_path) > 1:
        folder_to_concatenate = "/".join(bucket_split_path[1:]) + "/" + folder_to_concatenate
        result_filepath = "/".join(bucket_split_path[1:]) + "/" + result_filepath
    bucket = bucket_split_path[0]
    s3 = new_s3_client()
    parts_list = collect_parts(s3, bucket,folder_to_concatenate, file_suffix)
    logging.warning("Found {} parts to concatenate in {}/{}".format(len(parts_list), bucket, folder_to_concatenate))
    check = check_size(parts_list, max_filesize)
    if check:
        run_single_concatenation(s3, bucket, parts_list, "{}".format(result_filepath))
    else:
        local_appending(s3, bucket, parts_list, "{}".format(result_filepath))


def run_single_concatenation(s3, bucket, parts_list, result_filepath):
    if len(parts_list) > 1:
        # perform multi-part upload
        upload_id = initiate_concatenation(s3, bucket, result_filepath)
        parts_mapping = assemble_parts_to_concatenate(s3, bucket, result_filepath, upload_id, parts_list)
        complete_concatenation(s3, bucket, result_filepath, upload_id, parts_mapping)
    elif len(parts_list) == 0:
        # can perform a simple S3 copy since there is just a single file
        resp = s3.copy_object(Bucket=bucket, CopySource="{}/{}".format(bucket, parts_list[0][0]), Key=result_filepath)
        logging.warning("Copied single file to {} and got response {}".format(result_filepath, resp))
    else:
        logging.warning("No files to concatenate for {}".format(result_filepath))
        pass

def check_size(parts_list, max_filesize):
    current_list = []
    current_size = 0
    for p in parts_list:
        current_size += p[1]
        current_list.append(p)
        if current_size > max_filesize:
            return False
    return True

def new_s3_client():
    # initialize an S3 client with a private session so that multithreading
    # doesn't cause issues with the client's internal state
    session = boto3.session.Session()
    return session.client('s3')

def collect_parts(s3, bucket, folder, suffix):
    return list(filter(lambda x: x[0].endswith(suffix), _list_all_objects_with_size(s3, bucket, folder)))

def _list_all_objects_with_size(s3, bucket, folder):

    def resp_to_filelist(resp):
        return [(x['Key'], x['Size']) for x in resp['Contents']]

    objects_list = []
    resp = s3.list_objects(Bucket=bucket, Prefix=folder)
    objects_list.extend(resp_to_filelist(resp))
    while resp['IsTruncated']:
        # if there are more entries than can be returned in one request, the key
        # of the last entry returned acts as a pagination value for the next request
        logging.warning("Found {} objects so far".format(len(objects_list)))
        last_key = objects_list[-1][0]
        resp = s3.list_objects(Bucket=bucket, Prefix=folder, Marker=last_key)
        objects_list.extend(resp_to_filelist(resp))

    return objects_list

def initiate_concatenation(s3, bucket, result_filename):
    # performing the concatenation in S3 requires creating a multi-part upload
    # and then referencing the S3 files we wish to concatenate as "parts" of that upload
    resp = s3.create_multipart_upload(Bucket=bucket, Key=result_filename)
    logging.warning("Initiated concatenation attempt for {}, and got response: {}".format(result_filename, resp))
    return resp['UploadId']

def assemble_parts_to_concatenate(s3, bucket, result_filename, upload_id, parts_list):
    parts_mapping = []
    part_num = 0
    s3_parts = ["{}/{}".format(bucket, p[0]) for p in parts_list if p[1] > MIN_S3_SIZE]
    local_parts = [p[0] for p in parts_list if p[1] <= MIN_S3_SIZE]

    # assemble parts large enough for direct S3 copy
    for part_num, source_part in enumerate(s3_parts, 1): # part numbers are 1 indexed
        resp = s3.upload_part_copy(Bucket=bucket,
                                   Key=result_filename,
                                   PartNumber=part_num,
                                   UploadId=upload_id,
                                   CopySource=source_part)
        logging.warning("Setup S3 part #{}, with path: {}, and got response: {}".format(part_num, source_part, resp))
        parts_mapping.append({'ETag': resp['CopyPartResult']['ETag'][1:-1], 'PartNumber': part_num})

    # assemble parts too small for direct S3 copy by downloading them locally,
    # combining them, and then reuploading them as the last part of the
    # multi-part upload (which is not constrained to the 5mb limit)
    small_parts = []
    for source_part in local_parts:
        temp_filename = "/tmp/{}".format(source_part.replace("/","_"))
        s3.download_file(Bucket=bucket, Key=source_part, Filename=temp_filename)

        with open(temp_filename, 'rb') as f:
            small_parts.append(f.read())
        os.remove(temp_filename)
        logging.warning("Downloaded and copied small part with path: {}".format(source_part))

    if (len(small_parts) > 0) and (small_parts != [b'']):
        last_part_num = part_num + 1
        last_part = ''.join(small_parts)
        resp = s3.upload_part(Bucket=bucket, Key=result_filename, PartNumber=last_part_num, UploadId=upload_id, Body=last_part)
        logging.warning("Setup local part #{} from {} small files, and got response: {}".format(last_part_num, len(small_parts), resp))
        parts_mapping.append({'ETag': resp['ETag'][1:-1], 'PartNumber': last_part_num})

    return parts_mapping

def local_appending(s3, bucket, local_parts, result_filename):
    temp_all_filename = "/tmp/{}".format(result_filename.replace("/","_"))
    f_all = open(temp_all_filename,"a")
    for i, source_part in enumerate(local_parts):
        temp_filename = "/tmp/{}".format(source_part[0].replace("/","_"))
        s3.download_file(Bucket=bucket, Key=source_part[0], Filename=temp_filename)
        if i == 1:
            for line in open(temp_filename, "r+"):
                if (len(line) > 0) and (line != [b'']):
                    f_all.write(line)
        else:
            f = open(temp_filename, "r+")
            f.next() # skip the header
            for i, line in enumerate(f):
               if (len(line) > 0) and (line != [b'']) and (i==1):
                   f_all.write(line)
            f.close() # not really needed
        os.remove(temp_filename)

    f_all.close()
    resp = s3.upload_file(temp_all_filename, bucket, result_filename)
    os.remove(temp_all_filename)

def complete_concatenation(s3, bucket, result_filename, upload_id, parts_mapping):
    if len(parts_mapping) == 0:
        resp = s3.abort_multipart_upload(Bucket=bucket, Key=result_filename, UploadId=upload_id)
        logging.warning("Aborted concatenation for file {}, with upload id #{} due to empty parts mapping".format(result_filename, upload_id))
    else:
        resp = s3.complete_multipart_upload(Bucket=bucket, Key=result_filename, UploadId=upload_id, MultipartUpload={'Parts': parts_mapping})
        logging.warning("Finished concatenation for file {}, with upload id #{}, and parts mapping: {}".format(result_filename, upload_id, parts_mapping))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="S3 file combiner")
    parser.add_argument("--bucket", help="base bucket to use")
    parser.add_argument("--folder", help="folder whose contents should be combined")
    parser.add_argument("--output", help="output location for resulting merged files, relative to the specified base bucket")
    parser.add_argument("--suffix", help="suffix of files to include in the combination")
    parser.add_argument("--filesize", type=int, help="max filesize of the concatenated files in bytes", default=999999999)

    args = parser.parse_args()

    logging.warning("Combining files in {}/{} to {}/{}, with a max size of {} bytes".format(BUCKET, args.folder, BUCKET, args.output, args.filesize))
    BUCKET = args.bucket
    run_concatenation(args.folder, args.output, args.suffix, args.filesize)
