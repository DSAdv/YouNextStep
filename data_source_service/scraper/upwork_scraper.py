import datetime

import dateparser
import feedparser
import json
import pandas as pd

from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from urllib.parse import urlencode

SCRAPING_PAGE_NUM = 10
WORKERS_NUM = 100


def get_data_from_source(from_date: datetime.datetime):
    jobs = list()
    utc = datetime.timezone.utc

    for i in tqdm(range(SCRAPING_PAGE_NUM)):
        upwork_jobs = feedparser.parse(generate_upwork_rss_link(i * 100))
        jobs.extend(upwork_jobs["entries"])

    with ThreadPoolExecutor(max_workers=WORKERS_NUM) as pool:
        tasks = [pool.submit(prepare_parsed_record, job_json) for job_json in jobs]
        parsed_jobs_data = [task.result() for task in tqdm(tasks)]

    if from_date is not utc:
        from_date = from_date.replace(tzinfo=utc)

    df = pd.DataFrame(parsed_jobs_data)
    df["published"] = df["published"].map(lambda x: x.replace(tzinfo=utc))

    return df[df["published"] > from_date].to_dict(orient="records")


def generate_upwork_rss_link(offset: int = 1):
    base_rss_url = "https://www.upwork.com/ab/feed/jobs/rss"
    params = {
        "ontology_skill_uid": 996364628025274386,
        "sort": "recency",
        "paging": f"{offset};100",
        "api_params": 1,
        "securityToken": "0c55dee70809de57cac599ef078d98072982e99efb0c4cd7c342975af0b7beaa25bfc01203c3c54522f6bb650a74fb5ec54d3cd50016e78e036c9736ea53248c",
        "userUid": 874358620923199488,
        "orgUid": 874358620927393793,
    }

    rss_url = f"{base_rss_url}?{urlencode(params)}"
    return rss_url


def prepare_parsed_record(record: dict):
    return {
        "title": record["title"].strip("- Upwork"),
        "link": record["link"],
        "summary": record["summary"],
        "published": dateparser.parse(record["published"]),
        "parsed": datetime.datetime.now(),
    }
