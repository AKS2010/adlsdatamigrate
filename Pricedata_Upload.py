# -*- coding: utf-8 -*-
"""
Created on Wed Oct 18 14:37:38 2023

@author: AshokKumarSathasivam
"""

import pandas as pd

import time
import concurrent.futures
import threading
import logging
from ADLS2_Data_Upload import data_upload
from utils import connection_prop


# uuid_upload = uuid.uuid1()
uuid_upload = 'Pricedata_Upload'

# logger = logging.getLogger('Pricedata_Upload')
# logger.setLevel(logging.DEBUG)  # Set to DEBUG to catch all levels
# info_handler = logging.FileHandler('info.log')
# info_handler.setLevel(logging.INFO)
# info_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# info_handler.setFormatter(info_format)
# logger.addHandler(info_handler)


logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("app.log"),
                        # logging.StreamHandler()
                    ])

logger          = logging.getLogger(__name__)
lock            = threading.Lock()
conn_prop       = connection_prop()


powersource_query      = "Select * from powersource_mapping_datalake(nolock)"
powersource_details    = pd.read_sql(powersource_query, conn_prop.engine)

def status_update(datasetid,datatype,uploadby,status,category):
    cursor          = conn_prop.pyodbc_connection.cursor()
    try:
        with lock:
            cursor.execute(f"exec [adls_upload_status] {datasetid},'{category}',null,'{uploadby}',0,'{status}'").fetchall()
            # if status in ['COMP']:
            #     cursor.execute(f"exec [getDsIdDetails_datalake] {datasetid},'insert'").fetchall()
            cursor.close()
            conn_prop.pyodbc_connection.commit()
    except Exception as e:
        logger.error("Upload Status error occurred: %s", e)

def upload_chk(datasetid,datatype,uploadby):
    category = 'P' if datatype == "pricedata" else 'G'
    adls_exist_sql_query    = "SELECT datasetid FROM adls_uploaded_details where reupload_req = 0 and datasetid = "+datasetid+" and category = '"+category+"'"
    adls_exist_df           = pd.read_sql(adls_exist_sql_query, conn_prop.engine)
    return 'exist' if len(adls_exist_df)>0 else 'notexist'

def dataupload(datasetid,datatype,uploadby):
    
    global powersource_details
    
    datasetid = str(datasetid)
    category = 'P' if datatype == "pricedata" else 'G'
    start_time = time.time()
    
    data_exist_chk = upload_chk(datasetid,datatype,uploadby)
    
    if data_exist_chk == "exist":
        print('Data Already Exists for  '+str(datasetid))      
    else:        
        
        df = pd.DataFrame()
        if category =='P':
            sql_query   = "SELECT datetimekey,baseprice FROM pricedata where datasetid = "+datasetid
            df = pd.read_sql(sql_query, conn_prop.engine)
        if category =='G':
            sql_query   = "SELECT datetimekey,powersourceid,powercapacityfactor FROM generationdata where datasetid = "+datasetid
            df = pd.read_sql(sql_query, conn_prop.engine)
            df = df.pivot(index=['datetimekey'], columns='powersourceid', values='powercapacityfactor')
            df.reset_index(inplace=True)
            df = df.rename(columns={0:'Powersource3',1:'Powersource1',2:'Powersource2'})
            # df = df.rename(columns=dict(zip(powersource_details["db_source_name"], powersource_details["output_source_name"])))
        logger.info('Time Difference to Read data for '+str(datasetid)+' in ' + str(time.time() - start_time) )   
       
        inpr_status_thread = threading.Thread(target=status_update,args=(datasetid,datatype,uploadby,'INPR',category))
        inpr_status_thread.start()
        inpr_status_thread.join()

        result = data_upload(uuid_upload,datasetid,df,"insert",datatype)
        if result == "Success":
            comp_status_thread = threading.Thread(target=status_update,args=(datasetid,datatype,uploadby,'COMP',category))
            comp_status_thread.start()
            comp_status_thread.join()
            logger.info('Time Difference to Upload data for '+str(datasetid)+' in ' + str(time.time() - start_time) )   
        else:
            err_status_thread = threading.Thread(target=status_update,args=(datasetid,datatype,uploadby,'ERR',category))
            err_status_thread.start()
            err_status_thread.join()
            logger.error("ADLS Data Upload Failed for Dataset : %s ",datasetid)
        
    return 'Result - Time Difference to Upload for '+str(datasetid)+' in ' + str(time.time() - start_time) 

def main(countrycode):
        
    sql_query   = "select  *  from dataset_adls_up where  datasource='AFRY' and countrycode = '"+countrycode+"' and category in ('P','P,G') and datasetid not in (select datasetid from adls_upload_metrics where category='P')"
    df = pd.read_sql(sql_query, conn_prop.engine)
    
    datasetid = df['datasetid'].tolist()    
    
    datatype = "pricedata"
    uploadby = 'AKS'    
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        for datasetid in datasetid:
            futures.append(executor.submit(dataupload, datasetid=datasetid,datatype=datatype,uploadby=uploadby))
        for future in concurrent.futures.as_completed(futures):
            print(future.result())


# if __name__ == "__main__":
#     start_time = time.time()
#     main()
#     logger.info('Time Difference to Total Data Upload in ' + str(time.time() - start_time) )   