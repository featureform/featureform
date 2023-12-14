import os
import requests

# for local
# from dotenv import load_dotenv
# # Load .env file
# home_directory = os.getenv("HOME")
# env_path = os.path.join(home_directory, "Development/featureform/.env")
# load_dotenv(env_path)

# Configuration
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
HEADERS = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}


def delete_databricks_jobs():
    """List all jobs and trigger their deletion."""
    url = f"{DATABRICKS_HOST}/api/2.1/jobs/list"
    print('https' in url)
    print('.net' in url)
    response = requests.get(url, headers=HEADERS)

    if response.status_code == 200:
        jobs = response.json().get("jobs", [])
        delete_jobs(jobs)

        while response.json().get("has_more", False):
            next_token = response.json().get("next_page_token")
            response = requests.get(
                url, headers=HEADERS, params={"next_page_token": next_token}
            )
            delete_jobs(response.json().get("jobs", []))
    else:
        raise Exception(f"Error listing jobs: {response}")


def delete_jobs(jobs):
    """Delete a list of jobs."""
    for job in jobs:
        delete_job(job["job_id"])


def delete_job(job_id):
    """Delete a job by job ID."""
    print(f"Deleting job ID: {job_id}")
    url = f"{DATABRICKS_HOST}/api/2.1/jobs/delete"
    response = requests.post(url, headers=HEADERS, json={"job_id": job_id})

    if response.status_code == 200:
        print(f"Deleted job ID: {job_id}")
    else:
        raise Exception(f"Error deleting job {job_id}: {response.content}")


def main():
    delete_databricks_jobs()


if __name__ == "__main__":
    main()
