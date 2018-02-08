from urllib.parse import urlparse
import os


def url_to_local_path(url, local_dir):
    filename = os.path.basename(urlparse(url).path)
    return os.path.join(local_dir, filename)