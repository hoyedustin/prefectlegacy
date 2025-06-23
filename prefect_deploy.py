import httpx
from prefect import flow
from prefect.runner.storage import GitRepository
from prefect.blocks.system import Secret

if __name__ == "__main__":
    # use a personal access token stored in Secret block to access private Rook repo
    flow.from_source(
        source=GitRepository(
            url="https://github.com/rookcap/prefect-legacy.git",
            credentials={"access_token": Secret.load("github-access-token")}
        ),
        entrypoint="prefect_repo_info_flow.py:get_repo_info",
    ).deploy(
        name="legacy-managed-pool-deployment",
        work_pool_name="legacy-managed-pool",
    )