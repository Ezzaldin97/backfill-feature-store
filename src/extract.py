import sys
import os
sys.path.append(os.path.abspath("."))
import requests
from json import JSONDecodeError
import pandas as pd
from config_reader import Config
import logs_conf as log
from typing import Dict
import datetime

class GetData:
    def __init__(self,
                 ref_utc:datetime.datetime=datetime.datetime.utcnow().replace(minute=0, second=0, microsecond=0)) -> None:
        """
        Init Method

        Args:
        ref_utc: refernce datetime to set the export end date and export start date
        """
        self.api = Config().api
        self.ref_utc = ref_utc
    
    @staticmethod
    def query_parameters(reference_dt:datetime.datetime,
                         delay:int,
                         days_to_export:int) -> Dict:
        """
        set query parameters of API

        Args:
        refernce_dt: datetime object to set start and end dates of data.
        delay: time shift of api data
        days_to_export: number of days to be exported

        returns:
        params: dictionary of query parameters
        """
        start_dt = reference_dt - datetime.timedelta(days = delay+days_to_export)
        end_dt = reference_dt - datetime.timedelta(days = delay)
        params = {
            "offset":0,
            "sort":"HourUTC",
            "timezone": "utc",
            "start": start_dt.strftime("%Y-%m-%dT%H:%M"),
            "end": end_dt.strftime("%Y-%m-%dT%H:%M"),
        }
        return params
    
    def parse_data(self):
        """
        Parsing Data from API and Returns it as json file if it can be decoded.

        Return:
        Json Object
        """
        params = GetData.query_parameters(self.ref_utc, self.api["delay"], self.api["days_to_export"])
        log.LOG_GET.info(f"Start Parsing Data Between {params['start']} and {params['end']}")
        log.LOG_GET.info(f"Requesting Data From API")
        response = requests.get(url = self.api["url"],
                                params = params)
        log.LOG_GET.info(f"Status: {response.status_code}")
        try:
            response = response.json()
            log.LOG_GET.info(f"total records returned: {response['total']}")
            return response
        except JSONDecodeError:
            log.LOG_GET.error("Data Couldn't be Decoded")
            return None
    
    def convert_to_dataframe(self, data) -> pd.DataFrame:
        """
        Covert Json Object of Data to Pandas Dataframe

        Args:
        data: JSON Object of API Data

        returns:
        Pandas DataFrame of Data Records 
        """
        log.LOG_GET.info(f"Converting Data Records to DataFrame")
        records = data["records"]
        df = pd.DataFrame.from_records(records)
        return df