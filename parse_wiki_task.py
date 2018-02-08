import luigi
from luigi.contrib.spark import PySparkTask
import os
from unzip_task import UnzipTask
from parse_langlinks_task import ParseLanglinksTask

from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkContext, StorageLevel


class ParseWikiTask(luigi.Task):
    retry_count = 1

    #driver_memory = '10g'
    #executor_memory = '10g'
    #total_executor_cores = luigi.IntParameter(default=8, significant=False)
    #packages = 'com.databricks:spark-xml_2.11:0.4.1,org.mongodb.spark:mongo-spark-connector_2.11:2.2.0'

    # main language
    main_lang = luigi.Parameter()

    # secondary language to link translation
    trans_lang = luigi.Parameter()

    cache_dir = "./cache/"
    mongo_db = "mongodb://localhost:27017/wikipedia"

    @property
    def resources(self):
        return {'spark': 1}

    @property
    def pages_url(self):
        # .your.org doesn't limit to 3 concurrent downloads
        return 'https://dumps.wikimedia.your.org/{0}wiki/latest/{0}wiki-latest-pages-meta-current.xml.bz2'.format(
            self.main_lang)

    @property
    def pages_path(self):
        filename = '{0}wiki-latest-pages-meta-current.xml'.format(self.main_lang)
        return os.path.join(self.cache_dir, filename)

    @property
    def langlinks_url(self):
        return 'https://dumps.wikimedia.your.org/{0}wiki/latest/{0}wiki-latest-langlinks.sql.gz'.format(
            self.trans_lang)

    @property
    def langlinks_sql(self):
        filename = '{0}wiki-latest-langlinks.sql.gz'.format(self.trans_lang)
        return os.path.join(self.cache_dir, filename)

    @property
    def langlinks_csv(self):
        filename = '{0}wiki-latest-langlinks.csv'.format(self.trans_lang)
        return os.path.join(self.cache_dir, filename)

    def requires(self):
        return [UnzipTask(self.pages_url),
                ParseLanglinksTask(url=self.langlinks_url, sql_path=self.langlinks_sql, csv_path=self.langlinks_csv)]

    @staticmethod
    def segment_article(text):
        """Parse the article text

        Returns
        -------
        list of (str, str)
            Structure contains [(section_heading, section_content)].

        """
        import re
        from gensim.corpora.wikicorpus import filter_wiki

        lead_section_heading = "Introduction"
        top_level_heading_regex = r"\n==[^=].*[^=]==\n"
        top_level_heading_regex_capture = r"\n==([^=].*[^=])==\n"

        text = str(text)
        section_contents = re.split(top_level_heading_regex, text)
        section_headings = [lead_section_heading] + re.findall(top_level_heading_regex_capture, text)

        section_contents = [filter_wiki(section_content) for section_content in section_contents]
        sections = list(zip(section_headings, section_contents))
        return sections

    def run(self):
        #os.environ['PYSPARK_PYTHON'] = '/home/anton/anaconda3/bin/python'
        #os.environ['PYSPARK_DRIVER_PYTHON'] = '/home/anton/anaconda3/bin/python'
        SUBMIT_ARGS = '--packages com.databricks:spark-xml_2.11:0.4.1,' + \
                      'org.mongodb.spark:mongo-spark-connector_2.11:2.2.0 ' + \
                      '--driver-memory 12g --executor-memory 12g pyspark-shell'
        os.environ['PYSPARK_SUBMIT_ARGS'] = SUBMIT_ARGS

        sc = SparkContext('local[*]') #'spark://localhost:7077'
        sqlContext = SQLContext(sc)

        # incomplete xml schema, only needed parts
        xmlSchema = StructType([
            StructField("id", LongType(), True),
            StructField("ns", IntegerType(), True),
            StructField("redirect", StructType([
                StructField("_title", StringType(), True),
            ]), True),
            StructField("revision", StructType([
                StructField("text", StructType([
                    StructField("_VALUE", StringType(), True)
                ]), True),
                StructField("timestamp", StringType(), True)
            ]), True),
            StructField("title", StringType(), True)])

        pages = sqlContext.read.format('com.databricks.spark.xml').options(rowTag='page') \
            .load(self.pages_path, schema=xmlSchema) \
            .persist(StorageLevel.DISK_ONLY)
        
        langlinks = sqlContext.read.csv(self.langlinks_csv, header='true', inferSchema='true')\
                .where((col('lang')==self.main_lang) & (col('title').isNotNull())).select('id', 'title')\
                .withColumnRenamed('id', 'translation_id')

        # articles are in namespace 0
        articles = pages.where(col('ns') == 0).persist(StorageLevel.DISK_ONLY)

        # find redirect pages and group by the title they redirect to
        redirects = articles.where(col('redirect').isNotNull()) \
            .select('id', col('title').alias('from_title'), col('redirect._title').alias('to_title')) \
            .groupBy('to_title').agg(collect_set('from_title').alias('redirects'))

        # get article id, title, text and timestamp
        # then join with titles that redirect to each article
        # then join with ids of second language articles
        article_data = articles.where(col('redirect').isNull()) \
            .select('id', 'title', col('revision.text._VALUE').alias('text'),
                    col('revision.timestamp').alias('timestamp')) \
            .join(redirects.withColumnRenamed('to_title', 'title'), 'title', 'left_outer')\
        .join(langlinks, 'title', 'left_outer')

        # get talks id, title, text and timestamp
        talk_data = pages.where(col('ns') == 1).select('id', 'title', col('revision.text._VALUE').alias('text'),
                                                       col('revision.timestamp').alias('timestamp'))

        segment_udf = udf(self.segment_article,
                          ArrayType(StructType([
                              StructField("heading", StringType(), False),
                              StructField("content", StringType(), False)
                          ])))

        # segment article texts
        article_data = article_data.withColumn('sections', segment_udf('text'))

        # save articles and talks to local mongodb
        article_data.write.format("com.mongodb.spark.sql.DefaultSource") \
            .option("spark.mongodb.output.uri",
                    "{0}.{1}_articles".format(self.mongo_db, self.main_lang)) \
            .mode("overwrite") \
            .save()
        talk_data.write.format("com.mongodb.spark.sql.DefaultSource") \
            .option("spark.mongodb.output.uri",
                    "{0}.{1}_talks".format(self.mongo_db, self.main_lang)) \
            .mode("overwrite") \
            .save()

