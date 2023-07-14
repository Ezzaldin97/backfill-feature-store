import sys
import os
sys.path.append(os.path.abspath("."))
import datetime
from prefect import task, flow
from prefect.tasks import task_input_hash
import pandas as pd
from great_expectations.core import ExpectationSuite
import logs_conf as log
from extract import GetData
from transform import TransfromData
from validate import validator
from load import FeatureStoreLoader
import argparse
from typing import Union, List

@task(name = "ExtractAPIData",
      description = "Export Data From API and Convert it to DataFrame",
      retries = 3, retry_delay_seconds = 10,
      cache_key_fn = task_input_hash,
      cache_expiration = datetime.timedelta(minutes = 15),
      )
def export_api_data(ref_dt:datetime.datetime=datetime.datetime.utcnow().replace(minute=0, second=0, microsecond=0)) -> pd.DataFrame:
    if ref_dt:
        getter = GetData(ref_utc=ref_dt)
    else:
        getter = GetData()
    data = getter.parse_data()
    df = getter.convert_to_dataframe(data = data)
    return df

@task(name = "TransformRawData",
      description = "Transform Raw Data"
      )
def transfrom_raw_data(df:pd.DataFrame) -> pd.DataFrame:
    transformer = TransfromData(df)
    new_df = transformer.transform()
    return new_df

@task(name = "LoadData",
      description = "Load Transformed Data as a Feature Group to Hopsworks Feature Store")
def load_data(df: pd.DataFrame,
              validation_suite: ExpectationSuite,
              fg_name:str,
              fg_desc:str,
              fg_primary_key: Union[List[str], str],
              fg_event_time: str,
              feature_group_version: int,
              online_enabled: bool = False,
              overwrite: bool = False,
              wait_for_job: bool = False) -> None:
    loader = FeatureStoreLoader(df = df, validator = validation_suite)
    loader.feature_group_loader(
        name = fg_name,
        description = fg_desc,
        primary_key = fg_primary_key,
        event_time = fg_event_time,
        feature_group_version = feature_group_version,
        online_enabled = online_enabled,
        overwrite = overwrite,
        wait_for_job = wait_for_job
    )


@flow(name = "Pipeline",
      description = "Pipeline of Feature Store")
def pipline(ref_dt:datetime.datetime=datetime.datetime.utcnow().replace(minute=0, second=0, microsecond=0)) -> None:
    log.LOG_MAIN.info("Main Flow Started")
    df = export_api_data(ref_dt)
    transformed_df = transfrom_raw_data(df)
    load_data(df = transformed_df,
              validation_suite = validator,
              fg_name = "denmark_energy_consumption",
              fg_desc = "Denmark Hourly Enery Consumption Data",
              fg_primary_key = ["PriceArea", "ConsumerType_DE35"],
              fg_event_time = "HourUTC",
              feature_group_version = 1,
              wait_for_job = True)
    log.LOG_MAIN.info("Main Flow Finished")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Argumets of Feature Store Pipeline.')
    parser.add_argument('--reference_date',
                        required=False,
                        type = lambda s: datetime.datetime.strptime(s, '%Y-%m-%d').replace(tzinfo=datetime.timezone.utc),
                        help='reference datetime component')
    args = vars(parser.parse_args())
    if args["reference_date"]:
        pipline(args["reference_date"])
    else:
        pipline()