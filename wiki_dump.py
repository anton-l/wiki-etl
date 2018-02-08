import luigi
from parse_wiki_task import ParseWikiTask


class WikiDump(luigi.WrapperTask):
    def requires(self):
        yield ParseWikiTask(main_lang='ru', trans_lang='en')
        yield ParseWikiTask(main_lang='en', trans_lang='ru')
