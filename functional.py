import boto3
import pandas as pd
from io import StringIO, BytesIO #buffer inmemory to readcsv
from datetime import datetime, timedelta

#adapter layer

def list_files_in_prefix(bucket, prefix):
    files = [obj.key for obj in bucket.objects.filter(Prefix=prefix)]
    return files

def read_csv_to_df(bucket, key, decoding="utf-8", sep=","):
    csv_obj = bucket.Object(key=key).get().get('Body').read().decode(decoding)
    data = StringIO(csv_obj)
    df = pd.read_csv(data, delimiter=sep)
    return df

def write_df_to_s3(bucket, df, key):
    out_buffer = BytesIO()
    df.to_parquet(out_buffer, index=False)
    bucket.put_object(Body=out_buffer.getvalue(), Key=key)
    return True


def write_df_to_s3_csv(bucket, df, key):
    out_buffer = StringIO()
    df.to_csv(out_buffer, index=False)
    bucket.put_object(Body=out_buffer.getvalue(), Key=key)
    return True

## META layer - not core

def return_date_list(bucket, arg_date, src_format, meta_key):
    min_date = datetime.strptime(arg_date, src_format).date() - timedelta(days=1)
    today = datetime.today().date()

    try:
        df_meta = read_csv_to_df(bucket, meta_key)
        dates = [(min_date + timedelta(days=x)) for x in range(0, (today-min_date).days() + 1)]

        src_dates = set(pd.to_datetime(df_meta['source_date']).dt.date)

        dates_missing = set(dates[1:]) - src_dates
        if dates_missing:
            min_date = min(dates_missing) - timedelta(days=1)

            return_dates = [date.strftime(src_format) for date in dates  if date >= min_date]
            return_min_date = (min_date + timedelta(days=1)).strftime(src_format)
        else:
            return_dates = []
            return_min_date = datetime(2200, 1, 1).date()
    except bucket.session.client('s3').exceptions.NoSuchKey:
        return_dates = [(min_date + timedelta(days=x)).strftime(src_format) for x in range(0, (today-min_date).days() + 1)]
        return_min_date = arg_date
    
    return return_dates, return_min_date

def update_meta_file(bucket, meta_key, extract_date_list):
    df_new = pd.DataFrame(columns=['source_date', 'datetime_of_processing'])
    df_new['source_date'] = extract_date_list
    df_new['datetime_of_processing'] = datetime.today().strftime('%Y-%m-%D %H:%M:%S')
    df_old = read_csv_to_df(bucket, meta_key)
    df_all = pd.concat([df_old, df_new])
    write_df_to_s3_csv(bucket, df_all, meta_key)


# application layer

def extract(bucket, date_list):
    files =  [key for date in date_list for key in list_files_in_prefix(bucket, date)]
    df = pd.concat([read_csv_to_df(bucket, obj) for obj in files], ignore_index=True)
    return df

def tranform_report1(df, columns, arg_date):
    df = df.loc[:, columns]
    df.dropna(inplace=True)
    df['opening_price']= df.sort_values(by=['Time']).groupby(['ISIN', 'Date'])['StartPrice'].transform('first')
    df['closing_price']= df.sort_values(by=['Time']).groupby(['ISIN', 'Date'])['StartPrice'].transform('last')
    df = df.groupby(['ISIN', 'Date'], as_index=False).agg(opening_price_eur=('opening_price', 'min'), closing_price_eur=('closing_price', 'min'), minimum_price_eur=('MinPrice', 'min'), maximum_price_eur=('MaxPrice', 'max'), daily_traded_volume=('TradedVolume', 'sum'))
    df['prev_closing_price'] = df.sort_values(by=['Date']).groupby(['ISIN'])['closing_price_eur'].shift(1)
    df['change_prev_closing_%'] = (df['closing_price_eur'] - df['prev_closing_price'])/ df['prev_closing_price'] * 100
    df.drop(columns=['prev_closing_price'], inplace=True)
    df = df.round(decimals=2)
    df = df[df.Date >= arg_date]

    return df

def load(bucket, df, trg_key, trg_format, meta_key, extract_date_list):
    key = trg_key + datetime.today().strftime("%Y%m%d_%H%M%S") + trg_format
    write_df_to_s3(bucket, df, key)
    update_meta_file(bucket, meta_key, extract_date_list)
    return True


# ETL job
def etl_report1(src_bucket, trg_bucket, date_list, columns, arg_date, trg_key, trg_format, meta_key):
    df = extract(src_bucket, date_list)
    df = tranform_report1(df, columns, arg_date)
    extract_date_list = [date for date in date_list if date >= arg_date]
    load(trg_bucket, df, trg_key, trg_format, meta_key, extract_date_list)
    return True



# main function entrypoint

def main():
    # parameters/configurations
    # later read config

    arg_date = '2022-01-28'
    src_format = '%Y-%m-%d'
    src_bucket = 'deutsche-boerse-xetra-pds'
    trg_bucket = 'xetra-29012022'
    columns = ['ISIN', 'Date', 'Time', 'StartPrice', 'MaxPrice', 'MinPrice',
        'EndPrice', 'TradedVolume']

    trg_key = 'xetra_daily_report_'
    trg_format = '.parquet'
    meta_key = "meta_file.csv"

    # Init connections

    s3 = boto3.resource('s3')
    bucket_source = s3.Bucket(src_bucket)
    bucket_target = s3.Bucket(trg_bucket)

    # run application

    date_list, min_date = return_date_list(bucket_target, arg_date, src_format, meta_key)
    etl_report1(bucket_source, bucket_target, date_list, columns, min_date, trg_key, trg_format, meta_key)

# run 
if __name__ == "__main__":
    main()

