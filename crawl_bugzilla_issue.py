import requests
import json
import os
import pandas as pd
from argparse import ArgumentParser
from dotenv import load_dotenv
from tqdm import tqdm
from time import sleep
from utils import *

load_dotenv()


def fetch_bugzilla_issues(project_issue_url, project_name, limit=20, offset=0):

    headers = {
        "Accept": "application/json",
    }

    params = {
        "product": f"{project_name}",
        "limit": limit,
        "offset": offset,
    }

    response = requests.get(
        project_issue_url + "rest/bug",
        params=params,
        headers=headers,
    )

    assert response.status_code == 200, f"Failed to fetch issue {project_name}"

    return json.loads(response.text)


def fetch_bugzilla_issue_comments(project_issue_url, project_name, issue_id):

    headers = {
        "Accept": "application/json",
    }

    response = requests.get(
        project_issue_url + f"rest/bug/{issue_id}/comment",
        headers=headers,
    )

    assert (
        response.status_code == 200
    ), f"Failed to fetch issue {project_name}-{issue_id}"

    return json.loads(response.text)


def load_args():
    arg_parser = ArgumentParser()
    arg_parser.add_argument(
        "--project_issue_url", default="https://bugzilla.redhat.com/"
    )
    arg_parser.add_argument(
        "--project_name",
    )
    return arg_parser.parse_args()


if __name__ == "__main__":
    ## Load arguments
    args = load_args()

    ## Load data path
    data_folder = os.getenv("DATA")
    data_folder = os.path.join(data_folder, args.project_name)
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)
    raw_data_folder = os.path.join(data_folder, "raw")
    if not os.path.exists(raw_data_folder):
        os.makedirs(raw_data_folder)

    ## Load logger
    log_path = os.path.join(data_folder, "crawl.log")
    logger = Logger(name=args.project_name, log_file=log_path)

    ## Fetch total number of data
    info = fetch_bugzilla_issues(
        project_issue_url=args.project_issue_url,
        project_name=args.project_name,
        limit=1,
        offset=0,
    )
    total = info["total_matches"]

    ## Fetch all data
    json_issues = {}
    csv_issues = {
        "issue_id": [],
        "issue_sum": [],
        "issue_desc": [],
        "status": [],
        "url": [],
        "created_at": [],
        "updated_at": [],
    }
    logger.info(
        f"Start featching {total} issues from BUGZILLA ({args.project_issue_url}): {args.project_name}"
    )

    for page in tqdm(range(total // 20 + 1), desc=f"Project: {args.project_name}"):
        raw_data_file = os.path.join(
            raw_data_folder, f"{args.project_name}_{page}.json"
        )
        if os.path.exists(raw_data_file):
            data = load_json(raw_data_file)
        else:
            try:
                data = fetch_bugzilla_issues(
                    project_issue_url=args.project_issue_url,
                    project_name=args.project_name,
                    limit=20,
                    offset=page * 20,
                )
            except Exception as e:
                logger.error(f"Failed to fetch data from {args.project_name}: {e}")
                sleep(5)
            save_json(data, raw_data_file)
        for issue in data["bugs"]:
            id = issue["id"]
            if id not in csv_issues["issue_id"]:
                if "status" not in issue:
                    status = None
                    logger.warning(f"Status not found for issue {id}")
                else:
                    status = issue["status"]
                if "summary" not in issue:
                    summary = None
                    logger.warning(f"Summary not found for issue {id}")
                else:
                    summary = issue["summary"]
                if "creation_time" not in issue:
                    created_at = None
                    logger.warning(f"Creation time not found for issue {id}")
                else:
                    created_at = issue["creation_time"]
                if "last_change_time" not in issue:
                    updated_at = None
                    logger.warning(f"Last change time not found for issue {id}")
                else:
                    updated_at = issue["last_change_time"]
                comments = fetch_bugzilla_issue_comments(
                    project_issue_url=args.project_issue_url,
                    project_name=args.project_name,
                    issue_id=id,
                )
                if comments["bugs"][str(id)]["comments"]:
                    description = comments["bugs"][str(id)]["comments"][0]["text"]
                else:
                    description = None
                    logger.warning(f"Description not found for issue {id}")
                url = f"{args.project_issue_url}buglist.cgi?id={id}"
                csv_issues["issue_id"].append(id)
                csv_issues["issue_sum"].append(summary)
                csv_issues["issue_desc"].append(description)
                csv_issues["status"].append(status)
                csv_issues["url"].append(url)
                csv_issues["created_at"].append(created_at)
                csv_issues["updated_at"].append(updated_at)
                json_issues[id] = {
                    "issue_id": id,
                    "issue_sum": summary,
                    "issue_desc": description,
                    "status": status,
                    "url": url,
                    "created_at": created_at,
                    "updated_at": updated_at,
                }
            else:
                logger.warning(f"Duplicate issue found: {id}")

    ## Save data
    assert len(json_issues) == len(csv_issues["issue_id"]), "Data length mismatch"
    save_json(json_issues, os.path.join(data_folder, f"{args.project_name}.json"))
    df = pd.DataFrame.from_dict(csv_issues)
    df.to_csv(os.path.join(data_folder, f"{args.project_name}.csv"), index=False)
    logger.info(f"Saved {len(csv_issues['issue_id'])} issues to {data_folder}")
