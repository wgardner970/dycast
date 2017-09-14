import urllib
import fileinput
import logging
import boto3
import botocore
import rfc3986
import os

def read_file(file_url):
    file_uri = get_file_uri(file_url)

    if file_uri.scheme == "s3":
        return read_file_s3(file_uri)
    elif (file_uri.scheme == "http") or (file_uri.scheme == "https"):
        return read_file_http(file_url)
    elif (file_uri.scheme == "file") or (file_uri.scheme is None):
        return read_file_local(file_uri)
    else:
        raise ValueError(
            "File location '{0}' not supported".format(file_uri.scheme)
        )





### 'Private' methods

def read_file_s3(uri):
    logging.debug("Reading remote file...")

    boto3_session = boto3.Session()
    s3_client = boto3_session.client("s3")

    # uri.path includes a leading "/"
    try:
        response = s3_client.get_object(Bucket=uri.host, Key=uri.path[1:])
    except botocore.exceptions.ClientError, e:
        if e.response['Error']['Code'] == "404":
            logging.error("Requested file '%s' in bucket '%s' does not exist", uri.path[1:], uri.host)
        else:
            logging.error("There was a problem downloading requested file '%s' in bucket '%s'", uri.path[1:], uri.host)
        raise

    content = response["Body"].read()
    return content.splitlines()

def read_file_http(url):
    response = urllib.urlopen(url)
    if response.code == 200:
        return response.read()
    elif response.code == 404:
        logging.error("Requested file '%s' does not exist", url)
    else:
        logging.error("There was a problem downloading requested file '%s'", url)
    raise IOError

def read_file_local(uri):
    logging.debug("Reading local file...")
    file_path = get_file_path_from_uri(uri)
    try:
        input_file = fileinput.input(file_path)
    except IOError, e:
        logging.error("Failed to load file: %s", uri)
        raise e
    return input_file


def get_file_uri(url):
    return rfc3986.urlparse(url)


def get_file_path_from_uri(uri):
    if uri.host is None:
        return uri.path
    elif uri.path is None:
        return uri.host
    else:
        return os.path.join(uri.host, uri.path)