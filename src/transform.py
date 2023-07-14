import sys
import os
sys.path.append(os.path.abspath("."))
import pandas as pd
import logs_conf as log

class TransfromData:
    def __init__(self, df:pd.DataFrame) -> None:
        """
        Init Method

        Args:
        df: pandas DataFrame
        """
        self.df = df
        self.area_encoder = {"DK": 0, "DK1": 1, "DK2": 2}
    
    def transform(self) -> pd.DataFrame:
        log.LOG_TRANSFORM.info("Start Data Transformation Process")
        log.LOG_TRANSFORM.info("Drop Unnecessary Features")
        df = self.df.copy()
        df.drop("HourDK", axis = 1, inplace = True)
        log.LOG_TRANSFORM.info("Cast Data-Types of Features")
        df["HourUTC"] = pd.to_datetime(df["HourUTC"])
        df["ConsumerType_DE35"] = df["ConsumerType_DE35"].astype("int16")
        df["TotalCon"] = df["TotalCon"].astype("int64")
        log.LOG_TRANSFORM.info("Encode Categorical Features")
        df["PriceArea"] = df["PriceArea"].map(lambda x: self.area_encoder.get(x))
        df["PriceArea"] = df["PriceArea"].astype("int8")
        return df
    