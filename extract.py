import requests
import json
import os


def get_repositories(org_name: str, token: str):
    url = f"https://api.github.com/orgs/{org_name}/repos"

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3+json",
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching repositories: {response.status_code} - {response.text}")
        return None


def get_pull_requests(repo_name: str, org_name: str, token: str):
    url = f"https://api.github.com/repos/{org_name}/{repo_name}/pulls?state=all"

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3+json",
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        print(
            f"Error fetching pull requests for {repo_name}: {response.status_code} - {response.text}"
        )
        return None


def save_to_json(data: dict, file_name: str) -> None:
    os.makedirs("resources", exist_ok=True)
    full_path = os.path.join("resources", file_name)

    with open(full_path, "w") as outfile:
        json.dump(data, outfile, indent=4)


if __name__ == "__main__":
    org_name = "Scytale-exercise"
    token = os.getenv("GITHUB_TOKEN")

    if not token:
        print(
            "Error: GitHub token not found. Please set the GITHUB_TOKEN environment variable."
        )
        exit(1)

    repositories = get_repositories(org_name, token)

    if repositories:
        for repo in repositories:
            repo_name = repo["name"]
            pull_requests = get_pull_requests(repo_name, org_name, token)
            if pull_requests:
                filename = f"{repo_name}_pull_requests.json"
                save_to_json(pull_requests, filename)
                print(f"Pull requests for {repo_name} saved to resources/{filename}")
            else:
                print(f"No pull requests found for {repo_name}")
    else:
        print("No repositories found for the organization.")
