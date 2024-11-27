from argparse import ArgumentParser
from os import getenv, path, remove
from shutil import rmtree
from datetime import datetime, UTC
from pymongo import MongoClient, DeleteOne
import logging
from subprocess import run
import tarfile
from boto3 import client
from bson import decode_file_iter
import schedule
from time import sleep


def backup_and_upload(
        mongodb_uri: str,
        database_name: str,
        collection_name: str,
        aws_s3_bucket_name: str,
        aws_region: str,
        aws_access_key: str,
        aws_secret_key: str,
        tmp_dump_folder = "backup" # 'dump' is the default directory that mongodump outputs to
    ) -> None:
    # backup MongoDB collection using mongodump
    run(['mongodump', '--out', tmp_dump_folder, '--db', database_name, '--collection', collection_name, mongodb_uri])
    
    # read the backed-up docs into memory
    path_to_dump_file = path.join(tmp_dump_folder, database_name, f"{collection_name}.bson")
    with open(path_to_dump_file, 'rb') as file:
        backed_up_docs_to_delete = [DeleteOne({"_id": doc["_id"]}) for doc in decode_file_iter(file)]

    num_docs_backed_up = len(backed_up_docs_to_delete)

    if num_docs_backed_up <= 0:
        logging.info(f"Dump has no documents, nothing to do.")
        return

    # compress the backup to a .tar.gz file
    compressed_filename = f'{database_name}\\{collection_name}\\{datetime.now(UTC).isoformat()}.tar.gz' # can't use / in filename
    logging.debug(f"Compressing the dump to file {compressed_filename}...")
    with tarfile.open(compressed_filename, 'w:gz') as tar:
        tar.add(tmp_dump_folder)

    logging.debug(f"Compression successful")

    # upload the compressed backup to AWS S3
    logging.debug(f"Uploading file {compressed_filename} to s3 bucket {aws_s3_bucket_name} in region {aws_region}...")
    s3 = client('s3', region_name=aws_region, aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
    s3.upload_file(compressed_filename, aws_s3_bucket_name, compressed_filename) # TODO: handle errors here
    logging.debug(f"Upload successful")

    # delete all backed-up documents from the db
    logging.debug(f"Deleting {num_docs_backed_up} backed-up documents from database {database_name} and collection {collection_name}...")
    result = MongoClient(mongodb_uri)[database_name][collection_name].bulk_write(backed_up_docs_to_delete, ordered=False)
    logging.debug(f"Deletion successful: {result=}")

    # remove local files created above
    remove(compressed_filename)
    rmtree(tmp_dump_folder)

    logging.info(f"Backup completed (file {compressed_filename})")

    # to restore the backup directly into the database implement the following (untested) logic:
    #
    # # download the backup from s3
    # s3.download_file(aws_s3_bucket_name, compressed_filename, compressed_filename)
    # 
    # # extract the contents of the tar file
    # with tarfile.open(compressed_filename, 'r:gz') as tar:
    #     tar.extractall()
    #
    # # use mongorestore to add the documents directly into the database's collection
    # run(['mongorestore', mongodb_uri, f'{DUMP_FOLDER}/'])
    

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--hour_to_run_at", default=getenv("HOUR_TO_RUN_AT", 2), type=int, choices=list(range(24)))
    parser.add_argument("--mongodb_uri", default=getenv("MONGODB_URI", "mongodb://mongodb:27017"))
    parser.add_argument("--database_name", default=getenv("DB_NAME"))
    parser.add_argument("--collection_name", default=getenv("COLLECTION_NAME"))
    parser.add_argument("--aws_s3_bucket_name", default=getenv("AWS_S3_BUCKET_NAME"))
    parser.add_argument("--aws_region", default=getenv("AWS_REGION"))
    parser.add_argument("--aws_access_key", default=getenv("AWS_ACCESS_KEY"))
    parser.add_argument("--aws_secret_key", default=getenv("AWS_SECRET_KEY"))
    parser.add_argument("--tmp_dump_folder", default=getenv("TMP_DUMP_FOLDER", "backup"))
    parser.add_argument("--log_level", default=getenv("LOG_LEVEL", "INFO"))
    args = parser.parse_args()

    logging.basicConfig(level = args.log_level.upper()) # see https://docs.python.org/3/library/logging.html#levels

    logging.debug(f"{args=}")

    # backup_and_upload(
    #     args.mongodb_uri,
    #     args.database_name,
    #     args.collection_name,
    #     args.aws_s3_bucket_name,
    #     args.aws_region,
    #     args.aws_access_key,
    #     args.aws_secret_key,
    #     args.tmp_dump_folder
    # ) # run once at startup?

    schedule.every().day.at(f"0{args.hour_to_run_at}:00").do(
        backup_and_upload,
        args.mongodb_uri,
        args.database_name,
        args.collection_name,
        args.aws_s3_bucket_name,
        args.aws_region,
        args.aws_access_key,
        args.aws_secret_key,
        args.tmp_dump_folder
    )

    while True:
        if (n := schedule.idle_seconds()) > 0: # https://schedule.readthedocs.io/en/stable/examples.html#time-until-the-next-execution
            logging.debug(f"seconds until next execution: {n}")
            sleep(n)

        schedule.run_pending()
