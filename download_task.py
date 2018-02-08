import luigi
from utils import url_to_local_path
from urllib.request import urlretrieve
import os


class DownloadTask(luigi.Task):
    retry_count = 10

    url = luigi.Parameter()
    cache_dir = "./cache/"

    @property
    def file_path(self):
        return url_to_local_path(self.url, self.cache_dir)

    def output(self):
        return luigi.LocalTarget(self.file_path)

    def run(self):
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)

        urlretrieve(self.url, self.file_path)

    def on_failure(self, exception):
        try:
            os.remove(self.file_path)
        except OSError:
            pass
        super().on_failure(exception)
