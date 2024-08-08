# -*- coding: utf-8 -*-
"""
Created on Tue Aug  6 18:05:14 2024

@author: AshokKumarSathasivam
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Jul 29 12:05:12 2024

@author: AshokKumarSathasivam
"""



import time
import threading
# import concurrent.futures
import numpy as np
from utils import connection_prop
import logging
from Blob_storage_API_metrics import main

logger          = logging.getLogger(__name__)
conn_prop       = connection_prop()
lock            = threading.Lock()

# datasetid_list = [1,2,3]


def status_update(datasetid,price_min,price_max
                  ,price_sd,price_cnt,price_avg,price_nul
                  ,gen_cnt,datatype):
    cursor          = conn_prop.pyodbc_connection.cursor()
    
    # import pdb; pdb.set_trace()
    
    try:
        with lock:
            cursor.execute(f"exec [adls_metrics_sp] {datasetid},{price_min},{price_max},{price_sd},{price_cnt},{price_avg},{price_nul},{gen_cnt},'{datatype}'").fetchall()
            # if status in ['COMP']:
            #     cursor.execute(f"exec [getDsIdDetails_datalake] {datasetid},'insert'").fetchall()
            cursor.close()
            conn_prop.pyodbc_connection.commit()
    except Exception as e:
        logger.error("Upload Status error occurred: %s", e)

def upload_metrics(datasetid,datatype):
    
    if datatype == 'pricedata':
        datatype = 'P'
    if datatype == 'generationdata':
        datatype = 'G'
    
    st_time = time.time()
    
    price_min = 0
    price_max = 0
    price_sd = 0
    price_cnt = 0
    price_avg = 0
    price_nul = 0
    
    if datatype == 'P,G' or datatype == 'P':
        pricedataoutput = main('pricedata',datasetid)
        
        price_min = pricedataoutput['baseprice'].min()
        price_max = pricedataoutput['baseprice'].max()
        price_sd  = pricedataoutput['baseprice'].std()
        price_cnt = pricedataoutput['baseprice'].count()
        price_avg = pricedataoutput['baseprice'].mean()
        price_nul = pricedataoutput[pricedataoutput['baseprice'].isna()]['datetimekey'].count()
    
    gen_cnt = 0
    
    if datatype == 'P,G' or datatype == 'G':
        gendataoutput   = main('generationdata',datasetid)
        gen_cnt = gendataoutput['datetimekey'].count()
    # gen_min = pricedataoutput['baseprice'].min()
    # gen_max = pricedataoutput['baseprice'].max()
    # gen_sd  = pricedataoutput['baseprice'].std()
    # gen_avg = pricedataoutput['baseprice'].mean()
    # gen_nul = pricedataoutput[pricedataoutput['baseprice'].isna()]['datetimekey'].count()
    
    # import pdb; pdb.set_trace()
    
    price_min   = 0 if price_min    is np.nan else   round(price_min,4)
    price_max   = 0 if price_max    is np.nan else   round(price_max,4)
    price_sd    = 0 if price_sd     is np.nan else   round(price_sd,4)
    price_cnt   = 0 if price_cnt    is np.nan else   price_cnt
    price_avg   = 0 if price_avg    is np.nan  else  round(price_avg,4)
    price_nul   = 0 if price_nul    is np.nan else   price_nul
    gen_cnt     = 0 if gen_cnt      is np.nan else   gen_cnt
    
    inpr_status_thread = threading.Thread(target=status_update,args=(datasetid,price_min,price_max
                                                                     ,price_sd,price_cnt,price_avg,price_nul
                                                                     ,gen_cnt,datatype))
    inpr_status_thread.start()
    inpr_status_thread.join()
    print('Total Time Taken for'+str(datasetid)+' - '+ str(time.time()-st_time ))


# def main(datasetid,datatype):
#     upload_metrics(datasetid,datatype)

# with concurrent.futures.ThreadPoolExecutor(max_workers=35) as executor:
#     futures = []
#     datatype = 'G'
#     for datasetid in datasetid_list:
#         futures.append(executor.submit(upload_metrics, datasetid=datasetid,datatype=datatype))
#     for future in concurrent.futures.as_completed(futures):
#         print(future.result())


