from math import log
import ssl
from urllib.request import urlopen
from tempfile import _TemporaryFileWrapper


def download_catalog(url: str, fd: _TemporaryFileWrapper) -> None:
    ssl._create_default_https_context = ssl._create_unverified_context
    response = urlopen(url)
    while True:
        chunk: bytes = response.read(1024)
        if not chunk:
            break
        fd.write(chunk)


def get_concat_title_org(title: str, acronym: str, organization_name: str) -> str:
    concat = title
    if acronym:
        concat += ' ' + acronym
    if organization_name:
        concat += ' ' + organization_name
    return concat


def log2p(value):
    # Add 2 to the field value and take the common logarithm.
    # It makes sure that the result is > 0, needed for function score
    # Using multiply boost mode
    if not value:
        value = 0
    return log(value + 2)
