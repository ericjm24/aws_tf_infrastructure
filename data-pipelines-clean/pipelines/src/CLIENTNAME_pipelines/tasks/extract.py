from prefect import Task
import tarfile
import zipfile
import s3fs
from os import path
from functools import partial
from datetime import datetime

bufsize = 10485760  # 10 MB


def extract_tar(fileobj, s3_target, target_bucket, target_prefix, logger=None):
    written_files = []
    with tarfile.open(fileobj=fileobj, mode="r:gz") as tar:
        for member in tar:
            if member.isfile():
                fm = tar.extractfile(member)
                file_s3_name = f"s3://{target_bucket}/{target_prefix}/{member.name}"
                with s3_target.open(file_s3_name, "wb") as f_out:
                    while True:
                        buf = fm.read(bufsize)
                        if not buf:
                            break
                        f_out.write(buf)
                    written_files.append(file_s3_name)
                    if logger:
                        logger.info(f"Extracted file {member.name} to {file_s3_name}")
    return written_files


def extract_zip(fileobj, s3_target, target_bucket, target_prefix, logger=None):
    written_files = []
    with zipfile.ZipFile(fileobj, mode="r") as zip:
        for member in zip.infolist():
            if not member.is_dir():
                file_s3_name = f"s3://{target_bucket}/{target_prefix}/{member.filename}"
                with s3_target.open(file_s3_name, "wb") as f_out:
                    f_out.write(zip.read(member))
                    written_files.append(file_s3_name)
                    if logger:
                        logger.info(
                            f"Extracted file {member.filename} to {file_s3_name}"
                        )
    return written_files


def move_file(fileobj, s3_target, target_bucket, target_prefix, filename, logger=None):
    file_s3_name = f"s3://{target_bucket}/{target_prefix}/{filename}"
    with s3_target.open(file_s3_name, "wb") as f_out:
        while True:
            buf = fileobj.read(bufsize)
            if not buf:
                break
            f_out.write(buf)
        if logger:
            logger.info(f"Copied file {filename} to {file_s3_name}")
    return [file_s3_name]


def archive_file(
    source_bucket, source_key, s3_source, archive_bucket, s3_target, logger=None
):

    with s3_source.open(f"s3://{source_bucket}/{source_key}", "rb") as f:
        if source_key.endswith(".tar.gz"):
            sk = source_key.split(".", 2)[0]
            fx = "tar.gz"
        else:
            sk, fx = source_key.rsplit(".", 1)
        target_key = (
            f"{source_bucket}/{sk}_{datetime.now().strftime('%y%m%d_%H%M%S')}.{fx}"
        )
        with s3_target.open(f"s3://{archive_bucket}/{target_key}", "wb") as f_out:
            while True:
                buf = f.read(bufsize)
                if not buf:
                    break
                f_out.write(buf)
            if logger:
                logger.info(f"Archived file to s3://{archive_bucket}/{target_key}")


class ExtractTask(Task):
    def run(
        self,
        source_bucket=None,
        source_key=None,
        source_creds=None,
        target_bucket=None,
        target_prefix=None,
        target_creds=None,
        archive_bucket="CLIENTNAME-ds-data-archive",
        compression_type="infer",
    ):
        if target_bucket.startswith(("s3://", "s3a://", "s3n://")):
            target_bucket = target_bucket.split("//", 1)[-1].strip("/")
        else:
            target_bucket = target_bucket.strip("/")
        target_prefix = target_prefix.strip("/")
        if source_creds:
            s3_source = s3fs.S3FileSystem(
                key=source_creds.get("aws_access_key_id", None),
                secret=source_creds.get("aws_secret_access_key", None),
                token=source_creds.get("aws_token", None),
            )
        else:
            s3_source = s3fs.S3FileSystem()
        if source_creds == target_creds:
            s3_target = s3_source
        else:
            if target_creds:
                s3_target = s3fs.S3FileSystem(
                    key=target_creds.get("aws_access_key_id", None),
                    secret=target_creds.get("aws_secret_access_key", None),
                    token=target_creds.get("aws_token", None),
                )
            else:
                s3_target = s3fs.S3FileSystem()
        archive_file(
            source_bucket, source_key, s3_source, archive_bucket, s3_target, self.logger
        )
        if compression_type == "zip" or (
            compression_type == "infer" and source_key.endswith(".zip")
        ):
            extractor = extract_zip
        elif compression_type == "tar.gz" or (
            compression_type == "infer" and source_key.endswith(".tar.gz")
        ):
            extractor = extract_tar
        else:
            extractor = partial(move_file, filename=source_key.rsplit("/", 1)[-1])
        with s3_source.open(path.join(source_bucket, source_key), "rb") as f:
            written_files = extractor(
                fileobj=f,
                s3_target=s3_target,
                target_bucket=target_bucket,
                target_prefix=target_prefix,
                logger=self.logger,
            )
        return written_files
