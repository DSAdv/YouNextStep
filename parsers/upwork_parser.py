import feedparser
import json
import pandas as pd

from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from urllib.parse import urlencode


SCRAPING_PAGE_NUM = 38
WORKERS_NUM = 50


def get_data_from_source():
    jobs = []

    for i in tqdm(range(SCRAPING_PAGE_NUM)):
        upwork_jobs = feedparser.parse(generate_upwork_rss_link(i * 100))
        jobs.extend(upwork_jobs["entries"])

    return jobs


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


def parse_job_element(job_json: dict):
    return job_json


def main():
    jobs_data = get_data_from_source()

    with ThreadPoolExecutor(max_workers=WORKERS_NUM) as pool:
        tasks = [pool.submit(parse_job_element, job_json) for job_json in jobs_data]
        parsed_jobs_data = [task.result() for task in tqdm(tasks)]

    with open("upwork_jobs.json", "w") as f:
        json.dump(parsed_jobs_data, f)

