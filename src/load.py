import sys
import os
sys.path.append(os.path.abspath("."))
from config_reader import Config
import logs_conf as log 
import hopsworks
from great_expectations.core import ExpectationSuite
from hsfs.feature_group import FeatureGroup
import pandas as pd
from typing import Union, List

class FeatureStoreLoader:
    def __init__(self,
                 df: pd.DataFrame,
                 validator: ExpectationSuite) -> None:
        self.df = df
        self.validator = validator
        self.fs_credentials = Config().credentials

    @staticmethod
    def hopsworks_pointer(fs_api:str, fs_project_name:str):
        project = hopsworks.login(
            api_key_value = fs_api,
            project = fs_project_name
        )
        return project.get_feature_store()
    
    def feature_group_loader(self,
               name:str,
               description:str,
               primary_key: Union[List[str], str],
               event_time: str,
               feature_group_version: int,
               online_enabled: bool = False,
               overwrite: bool = False,
               wait_for_job: bool = False) -> FeatureGroup:
        log.LOG_FS.info("Create A Connection with HopsWorks Feature Store")
        fs_pointer = FeatureStoreLoader.hopsworks_pointer(self.fs_credentials["FS_API"],
                                                          self.fs_credentials["FS_PROJECT_NAME"])
        log.LOG_FS.info("Create Feature Group in Feature Store and Validate the Data.")
        energy_consumption_feature_group = fs_pointer.get_or_create_feature_group(
            name = name,
            description = description,
            primary_key = primary_key,
            event_time = event_time,
            version = feature_group_version,
            online_enabled = online_enabled,
            expectation_suite = self.validator
        )
        log.LOG_FS.info("Insert the Data in Feature Store and Validate it")
        energy_consumption_feature_group.insert(
            self.df,
            overwrite = overwrite,
            write_options = {
                "wait_for_job":wait_for_job
            }
        )

        log.LOG_FS.info("Computing Statistics of Data")
        energy_consumption_feature_group.statistics_config = {
        "enabled": True,
        "histograms": True,
        "correlations": True,
    }
        energy_consumption_feature_group.update_statistics_config()
        energy_consumption_feature_group.compute_statistics()
        return energy_consumption_feature_group