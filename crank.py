#!/usr/bin/env python3

# import boto3
# import glob
# import s3fs

from minio import Minio
from minio.commonconfig import CopySource
import sys
import argparse
import htcondor
import csv
import pathlib
import time
import os
import fnmatch

class S3Config:
    def __init__(self, akf, skf, endpoint):
        if not akf and not skf and not endpoint:
            home = os.getenv("HOME")
            with open(f"{home}/.chtc_s3/access.key", "r") as f:
                self.access_key = f.read().strip()
            with open(f"{home}/.chtc_s3/secret.key", "r") as f:
                self.secret_key = f.read().strip()

            self.access_key_file = f"{home}/.chtc_s3/access.key"
            self.secret_key_file = f"{home}/.chtc_s3/secret.key"
            self.endpoint = "s3dev.chtc.wisc.edu"

        elif akf and skf and endpoint:
            with open(akf, "r") as f:
                self.access_key = f.read().strip()
            with open(skf, "r") as f:
                self.secret_key = f.read().strip()

            self.access_key_file = akf
            self.secret_key_file = skf
            self.endpoint = endpoint

        else:
            raise Exception("Access/secret keys must be provided together with an endpoint.")

def submit_job(in_bucket, out_bucket, csv_file_list, s3conf):
    files = []
    for csv_file in csv_file_list:
        with open(csv_file, 'r', newline='') as csv_file:
            csv_reader = csv.reader(csv_file)
            for row in csv_reader:
                files.append(tuple(row))

    print("FILES TO BE COMPUTED ON: ", files)

    submit_description = htcondor.Submit({
        # Set up universe stuff
        "universe":                "container",
        "container_image":         "docker://osgeo/gdal:ubuntu-small-3.6.3",

        # Set up logging info
        "log":                     "$(CLUSTER).log",
        "output":                  "$(CLUSTER).out",
        "error":                   "$(CLUSTER).err",

        # Set up the stuff we actually run
        "executable":              "generate_cogs.sh",
        # "COGFILE":                 "cog_$(INFILE)",
        "arguments":               "$(INFILE) cog_$(INFILE) $(OUTFILE) $(BANDS)",

        # And requirements for running stuff
        "request_disk":            "1GB",
        "request_memory":          "1GB",
        "request_cpus":            1,
        
        # Set up the S3/file transfer stuff
        "aws_access_key_id_file":  f"{s3conf.access_key_file}",
        "aws_secret_access_key_file": f"{s3conf.secret_key_file}",
        "transfer_input_files":    f"s3://{s3conf.endpoint}/{in_bucket}/$(INFILE)",
        # After conversion, $INFILE is actually the generated COG, so we still want to transfer it to the output bucket
        "transfer_output_remaps":  f"\"$(INFILE) = s3://{s3conf.endpoint}/{out_bucket}/$(INFILE); $(OUTFILE) = s3://{s3conf.endpoint}/{out_bucket}/$(OUTFILE);\"",
        "should_transfer_files":   "YES",
        "when_to_transfer_output": "ON_EXIT",
    })

    input_args = [{"INFILE": files[idx][0], "OUTFILE": files[idx][1], "BANDS": files[idx][2]} for idx in range(len(files))]
    print("ABOUTY TO SUBMIT EP JOBS WITH INPUT ARGS: ", input_args)

    print("ABOUT TO SUBMIT EP JOB")
    schedd = htcondor.Schedd()
    submit_result = schedd.submit(submit_description, itemdata = iter(input_args))
    print("Job was submitted with JobID %d.0" % submit_result.cluster())


def submit_crondor(in_bucket, out_bucket, pattern, s3conf):
    script_path = str(pathlib.Path(sys.argv[0]).absolute())
    print("SCRIPT PATH: ", script_path)

    submit_description = htcondor.Submit({
        "executable":              script_path,
        "arguments":               f"crondor --input-bucket {in_bucket} --output-bucket {out_bucket} -p {pattern} -s {s3conf.secret_key_file} -a {s3conf.access_key_file} -e {s3conf.endpoint}",
        "universe":                "local",
        "request_disk":            "512MB",
        "request_cpus":            1,
        "request_memory":          512,
        "log":                     "crondor_$(CLUSTER).log",
        "should_transfer_files":   "YES",
        "when_to_transfer_output": "ON_EXIT",
        "output":                  "crondor_$(CLUSTER).out",
        "error":                   "crondor_$(CLUSTER).err",

        # Cron directives. Currently set to run every 15 minutes
        "cron_minute":             "*/15",
        "cron_hour":               "*",
        "cron_day_of_month":       "*",
        "cron_month":              "*",
        "cron_day_of_week":        "*",
        "on_exit_remove":          "false",

        # Specify `getenv` so that our script uses the appropriate environment
        # when it runs in local universe. This allows the crondor to access
        # modules we've installed in the base env (notable, minio)
        "getenv":                  "true",
    })

    schedd = htcondor.Schedd()
    submit_result = schedd.submit(submit_description)
    print("Job was submitted with JobID %d.0" % submit_result.cluster())

# Given a bucket, and a map of object renames, rename the objects in the bucket
def rename_object(bucket, obj_rename_map, s3conf):
    minioClient = Minio(s3conf.endpoint,
                        access_key=s3conf.access_key,
                        secret_key=s3conf.secret_key,
                        secure=True)

    for obj, new_obj in obj_rename_map.items():
        print(f"RENAMING {bucket}/{obj} TO {bucket}/{new_obj}")
        # There doesn't seem to be the concept of 'mv' with MinIO, so we copy/delete instead
        try:
            minioClient.copy_object(bucket, new_obj, CopySource(bucket, obj))
            minioClient.remove_object(bucket, obj)
        except Exception as e:
            raise Exception(f"Command failed: {str(e)}")
        
    # for obj, new_obj in obj_rename_map.items():
    #     cp_command = f"mc --config-dir <HOME DIR>/.mc cp {posixpath.join(bucket, obj)} {posixpath.join(bucket, new_obj)}"
    #     print("COPY COMMAND: ", cp_command)
    #     result = subprocess.run(cp_command, shell=True, env={"HOME": "<HOME DIR>"}, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #     if result.returncode != 0:
    #         raise Exception(f"Command failed with exit code {result.returncode}: {result.stderr}")
    #     else:
    #         print(f"OUTPUT: ", result.stdout)

    #     rm_command = f"mc --config-dir <HOME DIR>/.mc rm {posixpath.join(bucket, obj)}"
    #     print("COPY COMMAND: ", rm_command)
    #     result = subprocess.run(rm_command, shell=True, env={"HOME": "<HOME DIR>"}, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #     if result.returncode != 0:
    #         raise Exception(f"Command failed with exit code {result.returncode}: {result.stderr}")
    #     else:
    #         print(f"OUTPUT: ", result.stdout)


# Given a bucket and a list of objects, fetch the objects from the bucket
def fetch_objects(bucket, object_list, s3conf):
    minioClient = Minio(s3conf.endpoint,
                        access_key=s3conf.access_key,
                        secret_key=s3conf.secret_key,
                        secure=True)

    for obj in object_list:
        try:
            data = minioClient.get_object(bucket, obj)
            # print(data.read().decode('utf-8'))
            with open(obj, 'wb') as file_data:
                for d in data.stream(32*1024):
                    file_data.write(d)

        except Exception as e:
            raise Exception(f"Failed to fetch_objects: {str(e)}")

    # for obj in object_list:
    #     command = f"mc --config-dir <HOME DIR>/.mc cp {posixpath.join(bucket, obj)} ."
    #     print("COMMAND: ", command)
    #     result = subprocess.run(command, shell=True, env={"HOME": "<HOME DIR>"}, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #     if result.returncode != 0:
    #         raise Exception(f"Command failed with exit code {result.returncode}: {result.stderr}")


def get_matching_objects(bucket, pattern, s3conf):
    ################################
    # OPTION 4: Using minio-python #
    ################################
    # Initialize the Minio client
    minioClient = Minio(s3conf.endpoint,
                        access_key=s3conf.access_key,
                        secret_key=s3conf.secret_key,
                        secure=True)
    objects = minioClient.list_objects(bucket, recursive=True)
    # Filter the object names using the glob pattern
    files = [obj.object_name for obj in objects if fnmatch.fnmatch(obj.object_name, pattern)]

    return files

    # #############################
    # #   OPTION 2: Using MinIO   #
    # #############################
    # command = f"mc --config-dir <HOME DIR>/.mc find {bucket} --name \"{pattern}\""
    # #result = subprocess.run(command, shell=True, capture_output=True, text=True)
    # result = subprocess.run(command, shell=True, env={"HOME": "<HOME DIR>"}, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # if result.returncode != 0:
    #     raise Exception(f"Command failed with exit code {result.returncode}: {result.stderr}")

    # # A lambda expresion that conversts each element of the list to a string
    # files = list(map(lambda x: x.decode("utf-8").replace(bucket+"/", '', 1), result.stdout.splitlines()))
    # # files = result.stdout.splitlines()
    # return files

    #############################
    #   OPTION 1: Using boto3   #
    #############################
    # s3 = boto3.resource(
    #     service_name='s3',
    #     aws_access_key_id='<ACCESS_KEY>',
    #     aws_secret_access_key='<SECRET_KEY>',
    #     endpoint_url='https://s3dev.chtc.wisc.edu'
    # )

    # my_bucket = s3.Bucket(bucket)

    # files = []
    # for object in my_bucket.objects.all():
    #     if glob.fnmatch.fnmatch(object.key, pattern):
    #         files.append(object.key)

    # return files

    #############################
    #   OPTION 3: Using s3fs    #
    #############################
    # fs = s3fs.S3FileSystem(
    #     key='<ACCESS_KEY>',
    #     secret='<SECRET_KEY>',
    #     client_kwargs={'endpoint_url': 'https://s3dev.chtc.wisc.edu'}
    # )

    # all_files = fs.ls(bucket)
    # matching_files = [file for file in all_files if glob.fnmatch.fnmatch(file, pattern)]
    
    # return matching_files

def helperMain():
    parser = argparse.ArgumentParser(description="LACY TEST WORKFLOW TOOL")
    parser.add_argument("command", help="Helper command to run", choices=["crondor"])
    parser.add_argument("--input-bucket", help="The bucket to check for matching objects.")
    parser.add_argument("--output-bucket", help="The bucket to check for matching objects.")
    parser.add_argument("-p", "--pattern", help="The glob pattern to match against.")
    parser.add_argument("-s", "--secret-key", help="The secret key file to use for the S3 connection.")
    parser.add_argument("-a", "--access-key", help="The access key file to use for the S3 connection.")
    parser.add_argument("-e", "--s3-endpoint", help="The hostname of the s3 endpoint to connect to. Defaults to https://s3dev.chtc.wisc.edu.")
    args = parser.parse_args()

    s3conf = S3Config(args.access_key, args.secret_key, args.s3_endpoint)

    # Get any of the matching files. These contain info about the actual files we need to compute on.
    matching_files = get_matching_objects(args.input_bucket, args.pattern, s3conf)
    if len(matching_files) > 0:
        print("MATCHING FILES: ", matching_files)
        fetch_objects(args.input_bucket, matching_files, s3conf)
        print(f"Downloaded {len(matching_files)} files")

        # We now have the files, use them to submit the actual workflow
        submit_job(args.input_bucket, args.output_bucket, matching_files, s3conf)

        # Now that we've submitted the job, we rename the files in the bucket so we don't reprocess them
        rename_map = {}
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        for file in matching_files:
            # FOR NOW WE ASSUME EVERY FILE ENDS IN raw.csv
            # TODO: Make this work with the glob pattern
            rename_map[file] = f"{file[0:-7]}processing-{timestamp}.csv"

        rename_object(args.input_bucket, rename_map, s3conf)


def topMain():
    parser = argparse.ArgumentParser(description="LACY TEST WORKFLOW TOOL")
    parser.add_argument("--input-bucket", help="The bucket to check for matching objects.")
    parser.add_argument("--output-bucket", help="The bucket to check for matching objects.")
    parser.add_argument("-p", "--pattern", help="The glob pattern to match against.")
    parser.add_argument("-s", "--secret-key", help="The secret key file to use for the S3 connection.")
    parser.add_argument("-a", "--access-key", help="The access key file to use for the S3 connection.")
    parser.add_argument("-e", "--s3-endpoint", help="The hostname of the s3 endpoint to connect to. Defaults to https://s3dev.chtc.wisc.edu.")
    args = parser.parse_args()

    s3conf = S3Config(args.access_key, args.secret_key, args.s3_endpoint)
    submit_crondor(args.input_bucket, args.output_bucket, args.pattern, s3conf)
    return 0

def main():
    if len(sys.argv) > 1 and sys.argv[1] in ["crondor"]:
        helperMain()
        return 0

    return topMain()

if __name__ == '__main__':
    sys.exit(main())
