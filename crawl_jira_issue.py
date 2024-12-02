from argparse import ArgumentParser
from utils import *
from dotenv import load_dotenv
from time import sleep
from tqdm import tqdm
import os
import pandas as pd

load_dotenv()


def fetch_jira_issues(
    project_issue_url, project_key, index=0, max_results=50, component=None
):

    headers = {
        "Accept": "application/json",
    }

    params = {
        "jql": f"project='{project_key}'",
        "startAt": index,
        "maxResults": max_results,
    }

    if component:
        params["jql"] += f"&component={component}"

    response = requests.get(
        project_issue_url + "rest/api/2/search",
        params=params,
        headers=headers,
    )

    assert response.status_code == 200, f"Failed to fetch issue {project_key}-{index}"

    return json.loads(response.text)


def load_args():
    parser = ArgumentParser()
    parser.add_argument("--project_name", type=str)
    parser.add_argument("--project_key", type=str)
    parser.add_argument(
        "--project_issue_url",
        type=str,
        default="https://issues.apache.org/jira/",
    )
    parser.add_argument("--component", type=str, default=None)
    return parser.parse_args()


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
    info = fetch_jira_issues(
        project_issue_url=args.project_issue_url,
        project_key=args.project_key,
        index=0,
        max_results=1,
        component=args.component,
    )
    total = info["total"]

    ## Fetch data
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
        f"Start fetching {total} issues from JIRA ({args.project_issue_url}): {args.project_name}"
    )
    for page in tqdm(range(total // 50 + 1), desc=f"Project: {args.project_name}"):
        raw_data_file = os.path.join(raw_data_folder, f"{args.project_key}_{page}.json")
        if os.path.exists(raw_data_file):
            data = load_json(raw_data_file)
        else:
            try:
                data = fetch_jira_issues(
                    project_issue_url=args.project_issue_url,
                    project_key=args.project_key,
                    index=page * 50,
                    component=args.component,
                )
            except Exception as e:
                logger.error(f"Error fetching data at page {page}: {e}")
                sleep(5)
            save_json(data, raw_data_file)
        for issue in data["issues"]:
            id = issue["id"]
            if id not in csv_issues["issue_id"]:
                url = f"{args.project_issue_url}browse/{issue['key']}"
                if "status" not in issue["fields"]:
                    status = None
                    logger.warning(f"Missing status: {url} - {issue['self']}")
                else:
                    status = issue["fields"]["status"]["name"]
                if "summary" not in issue["fields"]:
                    summary = None
                    logger.warning(f"Missing summary: {url} - {issue['self']}")
                else:
                    summary = issue["fields"]["summary"]
                if "description" not in issue["fields"]:
                    description = None
                    logger.warning(f"Missing description: {url} - {issue['self']}")
                else:
                    description = issue["fields"]["description"]
                if "created" not in issue["fields"]:
                    created_at = None
                    logger.warning(f"Missing created_at: {url} - {issue['self']}")
                else:
                    created_at = issue["fields"]["created"]
                if "updated" not in issue["fields"]:
                    updated_at = None
                    logger.warning(f"Missing updated_at: {url} - {issue['self']}")
                else:
                    updated_at = issue["fields"]["updated"]

                json_issues[id] = {
                    "issue_id": id,
                    "issue_sum": summary,
                    "issue_desc": description,
                    "status": status,
                    "url": url,
                    "created_at": created_at,
                    "updated_at": updated_at,
                }
                csv_issues["issue_id"].append(id)
                csv_issues["issue_sum"].append(summary)
                csv_issues["issue_desc"].append(description)
                csv_issues["status"].append(status)
                csv_issues["url"].append(url)
                csv_issues["created_at"].append(created_at)
                csv_issues["updated_at"].append(
                    updated_at,
                )
            else:
                logger.warning(f"Duplicate issue: {issue['id']} - {issue['self']}")

    ## Save data
    assert len(json_issues) == len(csv_issues["issue_id"]), "Data mismatch"
    save_json(json_issues, os.path.join(data_folder, f"{args.project_name}.json"))
    df = pd.DataFrame.from_dict(csv_issues)
    df.to_csv(os.path.join(data_folder, f"{args.project_name}.csv"), index=False)
    logger.info(f"Saved {len(csv_issues["issue_id"])} issues to {data_folder}")
