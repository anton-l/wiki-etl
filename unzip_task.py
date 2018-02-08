import luigi
import os
from utils import url_to_local_path
from subprocess import call
from download_task import DownloadTask


class UnzipTask(luigi.Task):
    retry_count = 10

    url = luigi.Parameter()
    cache_dir = "./cache/"

    @property
    def zipped_path(self):
        return url_to_local_path(self.url, self.cache_dir)

    @property
    def unzipped_path(self):
        return os.path.splitext(self.zipped_path)[0]

    def requires(self):
        return DownloadTask(self.url)

    def output(self):
        return luigi.LocalTarget(self.unzipped_path)

    def run(self):
        # sudo apt-get install pbzip2
        call(["pbzip2", "-d ", self.zipped_path])

    def on_failure(self, exception):
        try:
            os.remove(self.unzipped_path)
        except OSError:
            pass
        super().on_failure(exception)
