from argparse import ArgumentParser
from utils import *
from dotenv import load_dotenv
from time import sleep
from tqdm import tqdm
from bs4 import BeautifulSoup
import os
import pandas as pd

load_dotenv()


def fetch_bugs_mysql_issues(type):
    url = "https://bugs.mysql.com/search-csv.php"

    params = {
        "status": "All",
        "severity": "all",
        "os": 0,
        "bug_age": 0,
        "order_by": "id",
        "direction": "ASC",
        "limit": "All",
        "mine": 0,
        "bug_type[]": type,
    }

    res = requests.get(url, params=params)

    assert res.status_code == 200, "Failed to fetch mysql issues"

    return res.text


def fetch_mysql_issue_description_comment(issue_id):
    url = f"https://bugs.mysql.com/bug.php?id={issue_id}"
    res = requests.get(url)
    assert res.status_code == 200, f"Failed to fetch mysql issue id={issue_id}"
    soup = BeautifulSoup(res.text, "html.parser")
    comments = soup.find_all("div", class_="comment")
    for comment in comments:
        if 'Description' in comment.text:
            return comment.text
    return None"


def load_args():
    parser = ArgumentParser()
    parser.add_argument("--type", type=str)
    return parser.parse_args()

if __name__ == "__main__":

    ## Load arguments
    args = load_args()

    ## Load data path
    data_folder = os.getenv("DATA")
    data_folder = os.path.join(data_folder, args.type)

    ## Fetch issues
    issues = fetch_bugs_mysql_issues(args.type)
    issues = issues.split("\n")

    ## Save issues
    issues_path = os.path.join(data_folder, "issues.csv")
    with open(issues_path, "w") as f:
        f.write("\n".join(issues))

    ## Fetch issue description and comments
    issues = pd.read_csv(issues_path)
    issues = issues["id"].tolist()

    issues_description = []
    issues_comments = []
    for issue in tqdm(issues):
        description = fetch_mysql_issue_description_comment(issue)
        issues_description.append(description)
        sleep(1)

    ## Save issues description and comments
    issues_description_path = os.path.join(data_folder, "issues_description.csv")
    with open(issues_description_path, "w") as f:
        f.write("\n".join(issues_description))