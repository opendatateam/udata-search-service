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
