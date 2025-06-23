import os

from prefect import flow, get_run_logger, task
from prefect.task_runners import SequentialTaskRunner
import requests

from configuration.config import Config
from load.paychex_company_loader import PaychexCompanyLoader
from load.paychex_worker_loader import PaychexWorkerLoader
from load.paychex_email_address_loader import PaychexEmailAddress, PaychexEmailAddressLoader

config = Config(os.getcwd())
config.rsa_private_key()


def authenticate():
    logger = get_run_logger()
    url = "https://api.paychex.com/auth/oauth/v2/token"

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    body = {
        "grant_type": "client_credentials",
        "client_id": config.paychex_client_id,
        "client_secret": config.paychex_client_secret,
    }

    try:
        response = requests.post(url, headers=headers, data=body)
        json_data = response.json()

        if "error" in json_data:
            raise Exception(json_data.get("error_description"))

        return json_data.get("access_token")
    except Exception as e:
        logger.error(e)
        raise e


def fetch_companies(access_token: str) -> list[dict]:
    logger = get_run_logger()
    url = "https://api.paychex.com/companies"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    try:
        response = requests.get(url, headers=headers)
        json_data = response.json()

        if "error" in json_data:
            raise Exception(json_data.get("error_description"))

        return json_data.get("content")
    except Exception as e:
        logger.error(e)
        raise e


def fetch_workers(access_token: str, company_id: str) -> list[dict]:
    base_url = f"https://api.paychex.com/companies/{company_id}/workers"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    offset, limit, workers = 0, 50, []
    try:
        while True:
            url = f"{base_url}?offset={offset}&limit={limit}"
            response = requests.get(url, headers=headers)
            json_data = response.json()
            if "error" in json_data:
                raise Exception(json_data.get("error_description"))

            content = json_data.get("content")
            content_item_count = len(content)
            workers.extend(content)

            pagination = json_data.get("metadata", {}).get("pagination")

            offset += content_item_count

            # If no items were fetched, or the new offset exceeds the total number of
            # items, break out of the loop.
            if content_item_count == 0 or offset >= pagination["total"]:
                break

    except Exception as e:
        raise e

    return workers


def fetch_worker_communications(access_token: str, worker_id: str) -> list[dict]:
    url = f"https://api.paychex.com/workers/{worker_id}/communications"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    try:
        response = requests.get(url, headers=headers)
        json_data = response.json()

        if "error" in json_data:
            raise Exception(json_data.get("error_description"))

        return json_data.get("content")

    except Exception as e:
        raise e


@task
def load_companies(access_token: str) -> list[dict]:
    logger = get_run_logger()
    companies = []
    try:
        companies = fetch_companies(access_token)
        PaychexCompanyLoader(config).write(companies)
    except Exception as e:
        logger.error(e)
        raise e

    return companies


@task
def load_workers(access_token: str, company_id: str) -> list[dict]:
    logger = get_run_logger()
    workers: list[dict] = []
    try:
        workers = fetch_workers(access_token, company_id)
        PaychexWorkerLoader(config).write(workers)
    except Exception as e:
        logger.error(e)
        raise e

    return workers


@task
def load_email_addresses(access_token: str, worker_ids: list[str]) -> list[PaychexEmailAddress]:
    # Unfortunately, the initial call to the Paychex Worker API does not include
    # email addresses. There also does not appear to be and endpoint to retrieve
    # email addresses for all workers in a single call. As a result, we need to
    # make a separate call to the Paychex Worker Communications API for each worker.
    logger = get_run_logger()
    email_addresses: list[PaychexEmailAddress] = []
    try:
        for worker_id in worker_ids:
            communications = fetch_worker_communications(access_token, worker_id)
            if not communications:
                continue

            data = [
                PaychexEmailAddress(r["communicationId"], worker_id, r["usageType"], r["uri"])
                for r in communications
                if r.get("type") == "EMAIL"
            ]

            email_addresses.extend(data)

        PaychexEmailAddressLoader(config).write(email_addresses)
    except Exception as e:
        logger.error(e)
        raise e

    return email_addresses


@flow
def paychex_payroll_flow(
    name="Paychex Payroll",
    description="Retrieves company and employee data from the Paychex API",
    task_runner=SequentialTaskRunner(),
):
    logger = get_run_logger()
    try:
        access_token = authenticate()
        companies = load_companies(access_token)
        for company in companies:
            workers = load_workers(access_token, company["companyId"])
            load_email_addresses(access_token, [worker["workerId"] for worker in workers])
    except Exception as e:
        logger.error(e)
        raise e


if __name__ == "__main__":
    paychex_payroll_flow()
