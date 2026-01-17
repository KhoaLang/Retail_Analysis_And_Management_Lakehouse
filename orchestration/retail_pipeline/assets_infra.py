from dagster import asset
import subprocess
import os

PROJECT_ROOT = os.path.abspath(os.getcwd())

@asset(
    description="Start core infrastructure services using docker-compose"
)
def start_infrastructure():
    """
    Starts Kafka, MinIO, Prometheus, Grafana, Trino
    """
    subprocess.run(
        [
            "docker",
            "compose",
            "up",
            "-d"
        ],
        cwd=PROJECT_ROOT,
        check=True
    )
