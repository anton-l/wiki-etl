# Wikipedia ETL process

## Purpose
This repo is a collection of scripts to load and transform article and talk data from Wikipedia dumps, as well as user data from Wikipedia API.

## How to setup
Requirements: python 3.5, spark 2.2, docker, docker-compose

Install python packages:
```
pip install luigi pyspark gensim
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

