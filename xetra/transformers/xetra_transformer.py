import logging
from typing import NamedTuple
from xetra.common.s3 import S3BucketConnector

class XetraSourceConfig(NamedTuple):

    src_first_extract_date : str
    src_columns : list
    src_col_date : str
    src_col_isin : str
    src_col_time : str
    src_col_start_price : str
    src_col_min_price : str
    src_col_max_price : str
    src_col_traded_vol :str


class XetraTargetConfig(NamedTuple):

    trg_col_isin : str
    trg_col_date : str
    trg_col_op_price : list
    trg_col_clos_price : str
    trg_col_min_price : str
    trg_col_max_price : str
    trg_col_dail_trad_vol : str
    trg_col_ch_prev_clos : str
    trg_key : str
    trg_key_date_format : str
    trg_format : str



class XetraETL():
    """
    Reads the Xetra data, transform and writes to target
    """

    def __init__(
        self, 
        s3_bucket_src : S3BucketConnector,
        s3_bucket_trg : S3BucketConnector,
        meta_key : str,
        src_args: XetraSourceConfig,
        trg_args: XetraTargetConfig
    ):

        self._logger = logging.getLogger(__name__)
        self.s3_bucket_src = s3_bucket_src
        self.s3_bucket_trg = s3_bucket_trg
        self.meta_key = meta_key
        self.src_args = src_args
        self.trg_args = trg_args

        self.extract_date = 
        self.extract_date_list = 
        self.meta_update_file = 

    def extract(self):
        pass
    
    def transform_report1(self):
        pass

    def load(self):
        pass

    def etl_report1(self):
        pass
