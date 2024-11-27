# auto_backup_mongodb_to_s3

This is a Python script that runs daily to back up a MongoDB collection to an s3 bucket. It does the following:
1. Downloads a backup of the entire specified collection
2. Compresses the backup to a tarball
3. Uploads it to the specified s3 bucket
4. Deletes all the documents present in the backup from the collection.

The name of the file stored in the s3 bucket is of the form `<database name>\<collection name>\<UTC ISO timestamp at which the backup was created>.tar.gz`.

You can pass command line args or use environment variable to customise various parameters in the code such as:
- MongoDB database and collection details
- AWS variables
- The hour at which the backup should run daily.

See the [.env.example](.env.example) file for a list of all the customizable params.

## Run locally

To run locally, first make sure that the following is installed (See the [Dockerfile](Dockerfile) for the relevant installation commands):
- MongoDB Database Tools
- Python >=3.11
- Dependencies in [requirements.txt](requirements.txt)

Once installed, run the code:

```bash
python3 main.py \
  --mongodb_uri ? \
  --database_name ? \
  --collection_name ? \
  --aws_s3_bucket_name ? \
  --aws_region ? \
  --aws_access_key ? \
  --aws_secret_key ? \
  --tmp_dump_folder backup \
  --log_level INFO
```

## Run using Docker

Build the dockerfile:

```bash
docker build -t auto_backup_mongodb_to_s3_image .
```

Populate an .env file with the required variables. You can use the [.env.example](.env.example) file as a template.

Run the docker container:

```bash
docker run --name auto_backup_mongodb_to_s3 --env-file .env  auto_backup_mongodb_to_s3_image
```
