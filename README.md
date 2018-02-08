# Wikipedia ETL process

## Purpose
This repo is a collection of scripts to load and transform article and talk data from Wikipedia dumps, as well as user data from Wikipedia API.

## How to setup
Requirements: python 3.5, spark 2.2, docker, docker-compose

Install python packages and pbzip2:
```
pip install luigi pyspark gensim
sudo apt-get install pbzip2
```

Start up the mongodb container:
```
docker-compose up
```

## How to run
### Articles and talks
Run via a local luigi scheduler:
```
PYTHONPATH='.' luigi --module wiki_dump WikiDump --local-scheduler --workers=4
```
### Users
Edit wiki language and mongo collection name in wiki_users.py:
```
language = 'en'
users_collection = con['wikipedia']['en_users']
```
Run the script:
```
python wiki_users.py
```

## How it works
### Articles and talks
A luigi pipeline is set up to download wikipedia XML page dumps and language link SQL dumps. They are transformed and combined using PySpark and then loaded into MongoDB. 

Fields of the created collection wikipedia.en_articles (wikipedia.ru_articles):
- id (int) // wikipedia page ID
- title (str)
- text (str) // text in wiki markup
- sections (str[]) // cleaned article sections
- timestamp (datetime)
- redirects (str[]) // titles of the pages that redirect to the article
- translation_id (int) // ID of the page in another language

### Users
wiki_users.py is set up to run continuously, fetching users from the wikipedia API and updating user records in MongoDB. If the script is stopped abruptly, it will continue from the last updated user in the database on the next run.

API docs: https://www.mediawiki.org/wiki/API:Allusers

Fields:
- userid
- name
- blockid
- blockedby
- blockedbyid
- blockedtimestamp
- blockreason
- blockexpiry
- editcount
- registration
- groups
- updated // added field to track progress