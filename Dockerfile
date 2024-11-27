# Use the official Python base image
FROM python:slim

# Install MongoDB Tools
# some of mongodb-database-tools.deb's required dependencies are not installed in this image which causes its installation to fail, but installing the krb5-user (kerberos) package solves that problem
RUN apt update && \
    apt-get install -y wget krb5-user

RUN wget -qO mongodb-database-tools.deb https://fastdl.mongodb.org/tools/db/mongodb-database-tools-debian12-x86_64-100.10.0.deb && \
    dpkg -i mongodb-database-tools.deb && \
    apt install -f && \
    rm mongodb-database-tools.deb

# Set the working directory
WORKDIR /app

# Install Python dependencies
RUN pip install boto3 pymongo schedule

# Copy the script and requirements.txt into the container
COPY main.py .

# Set the entrypoint to run the script
ENTRYPOINT ["python", "/app/main.py"]