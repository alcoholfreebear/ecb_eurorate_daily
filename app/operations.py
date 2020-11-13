import pandas as pd
import pandas_gbq
import numpy as np
from google.cloud import bigquery

def load_data(opt:str='new'):
    """
    opt: {'hist', 'new'}
    """
    urls = {
        "hist":"https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip",
        "new":"https://www.ecb.europa.eu/stats/eurofxref/eurofxref.zip"}
    hist= pd.read_csv(urls[opt], compression='zip')
    return hist


def process_data(hist:pd.DataFrame)->pd.DataFrame:
    value_cols = [x for x in hist.columns  if (x.strip() not in ['Date', ''])and('Unnamed' not in x)]
    hist = hist.pipe(pd.melt,
                    id_vars=['Date'],
                    var_name='CurrencyCode',
                    value_vars=value_cols,
                    value_name='EuroToCurrency')
    hist['CurrencyToEuro']=np.round(1/hist['EuroToCurrency'], decimals=6)
    hist['Date']=pd.to_datetime(hist['Date'], utc=True)
    hist['CurrencyCode']=hist['CurrencyCode'].str.strip()
    return hist.sort_values(by='Date', ascending=False)

def data_is_new(indata, project_id):
    indata=load_data('new')
    indata=process_data(indata)
    indata['Date'].max()
    max_hist_date=pandas_gbq.read_gbq(f"""
            SELECT max(Date) FROM `{project_id}.currency_exchange.euro_rates_daily`
            """).values.item()
    return indata['Date'].max()>max_hist_date

def upload_to_bq(hist_bq:pd.DataFrame, project_id, if_exists:str='replace'):
    project_id = 'ds-smartsupply'
    dataset_id = 'currency_exchange'
    table_id = 'euro_rates_daily'
    pandas_gbq.to_gbq(hist_bq,  f'{dataset_id}.{table_id}', project_id=project_id, if_exists=if_exists, location='europe-west2')
    message_string = f'{len(hist_bq)} rows uploaded to table `{dataset_id}.{table_id}`'
    print(message_string)
    return({'status':message_string})


def seed_historical(project_id):
    try:
        hist = load_data('hist')
        hist = process_data(hist)
        upload_to_bq(hist, project_id)
        return 'success'
    except: return 'failure'

def append_new(project_id):
    try:
        new = load_data('new')
        new = process_data(new)
        if data_is_new(indata=new,  project_id=project_id):
            upload_to_bq(new, if_exists='append')
        else:
            print('no new data to append')
        return 'success'
    except:
        return 'failure'

