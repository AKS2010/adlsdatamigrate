
import pandas as pd
import time

from io import StringIO
import logging

from utils import connection_prop
from ADLS_Upload_metrics_Single import upload_metrics
# Create a custom logger

logger = logging.getLogger(__name__)



def validate_dataset(datasetid, datatype, upload_type, input_df):
    conn_prop = connection_prop()

    # dataset_columns = input_df.columns

    if upload_type == "insert":

        cursor = conn_prop.pyodbc_connection.cursor()
        proc_output = cursor.execute(
            f"exec [getDsIdDetails_datalake] {datasetid}")
        dataset = proc_output.fetchall()
        column_names = [column[0] for column in proc_output.description]
        dataset_details = pd.DataFrame(
            [tuple(row) for row in dataset], columns=column_names)
        cursor.close()
        conn_prop.pyodbc_connection.commit()

        dataset_row_count = dataset_details['TotalRows'][0]

        if dataset_row_count > 0:
            raise ValueError(
                f'Data Aleady Exists For Dataset-{datasetid}. Please use Append or Relace.')

    if upload_type == "append":

        cursor = conn_prop.pyodbc_connection.cursor()
        proc_output = cursor.execute(
            f"exec [getDsIdDetails_datalake] {datasetid}")
        dataset = proc_output.fetchall()
        column_names = [column[0] for column in proc_output.description]
        dataset_details = pd.DataFrame(
            [tuple(row) for row in dataset], columns=column_names)
        cursor.close()
        conn_prop.pyodbc_connection.commit()

        dataset_row_count = dataset_details['TotalRows'][0]

        if dataset_row_count == 0:
            raise ValueError(
                f'No Data Exists For Dataset-{datasetid}. Please use Insert.')


def insert_data(datasetid, input_df, upload_type, datatype):
    conn_prop = connection_prop()

    datatypecolumn = 'price_adls_loc'
    
    if datatype == "pricedata":
        datatypecolumn = 'price_adls_loc'
    
    if datatype == "generationdata":
        datatypecolumn = 'generation_adls_loc'
    
    dataset_details_query = "Select "+datatypecolumn+" From dataset_adls(nolock) where datasetid="+str(datasetid)

    dataset_details = pd.read_sql(dataset_details_query, conn_prop.engine)

    # market_type = dataset_details['market_type'].tolist()[0]
    # countrycode = dataset_details['countrycode'].tolist()[0]
    # locationcode = dataset_details['locationcode'].tolist()[0]
    # weatheryear = dataset_details['weatheryear'].tolist()[0]

    # blob_name = datatype+"/"+market_type+"/"+countrycode + \
    #     "/"+locationcode+"/"+weatheryear+"/"+datasetid+".csv"
    
    blob_name = dataset_details[datatypecolumn][0]
    
    
    input_df = input_df.sort_values('datetimekey')
    
    try:
        container_client = conn_prop.blob_service_client.get_container_client(
            conn_prop.container_name)
        blob_client = container_client.get_blob_client(blob_name)

        blob_client.upload_blob(input_df.to_csv(
            index=False, header=True), overwrite=True)
        
        upload_metrics(datasetid,datatype)
        return "Success"
    except Exception as e:
        logger.error("Insert Data Error occurred: %s", e)
        return "Failure"
    finally:
        logger.info(
            'Data Upload Process Completed in ADLS for dataset : %s', datasetid)
    #     logger.info('This is an info message')   # Will be logged to info.log
    #     logger.error('Error Time Difference to Upload data for '+str(datasetid)+' in ' + str(time.time() - start_time) )
    # try:
    #     blob_client = container_client.get_blob_client(blob_name)
    #     # upload data
    #     block_list=[]
    #     total_len_df = len(input_df)

    #     chunk_size = round((total_len_df / 324120),0) if total_len_df  > 324120 else 1

    #     for df in np.array_split(input_df, chunk_size):
    #         blk_id  = str(uuid.uuid4())
    #         csv_string = df.to_csv(index=False,header=False)
    #         csv_bytes = str.encode(csv_string)
    #         # data_chunk = b"\n".join([row.encode() for row in df])
    #         blob_client.stage_block(block_id=blk_id,data=csv_bytes)
    #         block_list.append(BlobBlock(block_id=blk_id))
    #     blob_client.commit_block_list(block_list)

    # except BaseException as err:
    #     print('Upload file error')
    #     print(err)


def append_data(datasetid, input_df, upload_type, datatype):
    conn_prop = connection_prop()
    
    datatypecolumn = 'price_adls_loc'
    
    if datatype == "pricedata":
        datatypecolumn = 'price_adls_loc'
    
    if datatype == "generationdata":
        datatypecolumn = 'generation_adls_loc'
    
    dataset_details_query = "Select "+datatypecolumn+" From dataset_adls(nolock) where datasetid="+str(datasetid)

    dataset_details = pd.read_sql(dataset_details_query, conn_prop.engine)

    # market_type = dataset_details['market_type'].tolist()[0]
    # countrycode = dataset_details['countrycode'].tolist()[0]
    # locationcode = dataset_details['locationcode'].tolist()[0]
    # weatheryear = dataset_details['weatheryear'].tolist()[0]

    # blob_name = datatype+"/"+market_type+"/"+countrycode + \
    #     "/"+locationcode+"/"+weatheryear+"/"+datasetid+".csv"
    
    blob_name = dataset_details[datatypecolumn][0]
    data_container_client = conn_prop.blob_service_client.get_container_client(
        conn_prop.container_name)
    data_blob_client = data_container_client.get_blob_client(blob_name)

    stream = data_blob_client.download_blob().content_as_text()
    existing_df = pd.read_csv(StringIO(stream), header=None)

    existing_df.columns = ['Timestamp',
                           'datasetid', 'PriceContext', 'BasePrice']

    appended_df = pd.concat([existing_df, input_df])

    data_blob_client.upload_blob(appended_df.to_csv(
        index=False, header=True), overwrite=True)


def data_upload(uuid_upload, datasetid, input_df, upload_type, datatype):
    start_time = time.time()
    # try :
    #     validate_dataset(datasetid,datatype,upload_type,input_df)
    #     ['Timestamp','datasetid','PowerSource','PowerCapacityFactor']
    # except Exception  as e :
    #     logger.error("Insert Data Error occurred: %s - %s",  datasetid, e)

    if upload_type == "insert":
        result = insert_data(datasetid, input_df, upload_type, datatype)

    if upload_type == "append":
        result = append_data(datasetid, input_df, upload_type, datatype)

    if upload_type == "replace":
        result = insert_data(datasetid, input_df, upload_type, datatype)

    logger.info('Time Difference to Upload data for ' +
                str(datasetid)+' in ' + str(time.time() - start_time))
    return(result)
