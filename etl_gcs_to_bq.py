from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=0, log_prints=True)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("my-gcs-bucket")
    print(f'gcs_path 1: {gcs_path}')
    gcs_block.get_directory(from_path=gcs_path, local_path='')
    print(f'gcs_path 2: {gcs_path}')
    return Path(gcs_path)


@task()
def extract(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    # print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    # df["passenger_count"].fillna(0, inplace=True)
    # print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("my-gcs-creds")

    df.to_gbq(
        destination_table="my_dataset.more-rides",
        project_id="dtc-de-376923",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow(log_prints=True)
def etl_gcs_to_bq(month: int, year: int, color: str):
    """Main ETL flow to load data into Big Query"""
    path = extract_from_gcs(color, year, month)
    print(f'path: {path}')
    df = extract(path)
    print(f'Number of rows: {len(df)}')
    write_bq(df)

@flow()
def parent_etl_flow(months: list[int], year: int, color: str):
    for month in months:
        etl_gcs_to_bq(month=month, year=year, color=color)

if __name__ == "__main__":
    months=[1]
    year=2021
    color='green'

    parent_etl_flow(months=months, year=year, color=color)