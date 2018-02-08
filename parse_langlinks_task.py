import luigi
import os
from download_task import DownloadTask
from sql_to_csv import sql_to_csv


class ParseLanglinksTask(luigi.Task):
    """
    Downloads language links sql dump and converts it to csv
    """
    retry_count = 10

    url = luigi.Parameter()
    sql_path = luigi.Parameter()
    csv_path = luigi.Parameter()

    cache_dir = "./cache/"

    def requires(self):
        return DownloadTask(self.url)

    def run(self):
        sql_to_csv(self.sql_path, self.csv_path)

    def output(self):
        return luigi.LocalTarget(self.csv_path)

    def on_failure(self, exception):
        try:
            os.remove(self.csv_path)
        except OSError:
            pass
        super().on_failure(exception)