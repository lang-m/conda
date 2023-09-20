# Copyright (C) 2012 Anaconda, Inc
# SPDX-License-Identifier: BSD-3-Clause
"""Defines S3 transport adapter for CondaSession (requests.Session)."""
import json
from logging import LoggerAdapter, getLogger
from tempfile import SpooledTemporaryFile

boto3 = None


def _load_boto3():
    """
    Import boto3 on demand only to save startup time.
    """
    global boto3

    try:
        import boto3
    except ImportError:
        pass


from ....common.compat import ensure_binary
from ....common.url import url_to_s3_info
from .. import BaseAdapter, CaseInsensitiveDict, Response

log = getLogger(__name__)
stderrlog = LoggerAdapter(getLogger("conda.stderrlog"), extra=dict(terminator="\n"))


class S3Adapter(BaseAdapter):
    def __init__(self):
        super().__init__()

    def send(
        self, request, stream=None, timeout=None, verify=None, cert=None, proxies=None
    ):
        resp = Response()
        resp.status_code = 200
        resp.url = request.url

        if not boto3:
            _load_boto3()

        if boto3:
            return self._send_boto3(boto3, resp, request)
        else:
            stderrlog.info(
                "\nError: boto3 is required for S3 channels. "
                "Please install with `conda install boto3`\n"
                "Make sure to run `conda deactivate` if you "
                "are in a conda environment.\n"
            )
            resp.status_code = 404
            return resp

    def close(self):
        pass

    def _send_boto3(self, boto3, resp, request):
        from botocore.exceptions import BotoCoreError, ClientError

        bucket_name, key_string = url_to_s3_info(request.url)
        # https://github.com/conda/conda/issues/8993
        # creating a separate boto3 session to make this thread safe
        session = boto3.session.Session()
        # create a resource client using this thread's session object
        s3 = session.resource("s3")
        # finally get the S3 object
        key = s3.Object(bucket_name, key_string[1:])

        try:
            response = key.get()
        except (BotoCoreError, ClientError) as e:
            resp.status_code = 404
            message = {
                "error": "error downloading file from s3",
                "path": request.url,
                "exception": repr(e),
            }
            resp.raw = self._write_tempfile(
                lambda x: x.write(ensure_binary(json.dumps(message)))
            )
            resp.close = resp.raw.close
            return resp

        key_headers = response["ResponseMetadata"]["HTTPHeaders"]
        resp.headers = CaseInsensitiveDict(
            {
                "Content-Type": key_headers.get("content-type", "text/plain"),
                "Content-Length": key_headers["content-length"],
                "Last-Modified": key_headers["last-modified"],
            }
        )

        resp.raw = self._write_tempfile(key.download_fileobj)
        resp.close = resp.raw.close

        return resp

    def _write_tempfile(self, writer_callable):
        fh = SpooledTemporaryFile()
        writer_callable(fh)
        fh.seek(0)
        return fh
