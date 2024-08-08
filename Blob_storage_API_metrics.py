# -*- coding: utf-8 -*-
"""
Created on Tue Aug  6 16:31:43 2024

@author: AshokKumarSathasivam
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Jun 12 14:03:11 2024

@author: AshokKumarSathasivam
"""

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
import pandas as pd
from io import StringIO
import json
import sqlalchemy
import os 
import urllib

from utils import connection_prop


def generation_data_output(datasetid,datatype):
    # Define your Azure AD credentials
    
    conn_prop = connection_prop()
    
    tenant_id       = conn_prop.tenant_id
    client_id       = conn_prop.client_id
    client_secret   = conn_prop.client_secret
    
    datatypecolumn = 'price_adls_loc'
    
    if datatype == "pricedata": 
        datatypecolumn = 'price_adls_loc'
    
    if datatype == "generationdata":
        datatypecolumn = 'generation_adls_loc'
    
    # dataset_details_query = 	"Select '"+datatype+"'+'/'+market_type+'/'+countrycode+'/'+locationcode+'/'+weatheryear+'/'+convert(nvarchar,datasetid)+'_'+market_type+'_'+countrycode+'_'+locationcode+'_'+weatheryear+'.csv'  as 'file_output' From dataset(nolock) where datasetid="+str(datasetid)
    dataset_details_query = 	"Select "+datatypecolumn+" From dataset_adls(nolock) where datasetid="+str(datasetid)
    dataset_details = pd.read_sql(dataset_details_query, conn_prop.engine)
    
    # Define your storage account details
    storage_account_name    = conn_prop.storage_account_name
    filesystem_name         = conn_prop.filesystem_name
    directory_path          = dataset_details[datatypecolumn][0]
    
    # Get a credential object
    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    
    # Create a DataLakeServiceClient object
    service_client = DataLakeServiceClient(
        account_url=f"https://{storage_account_name}.dfs.core.windows.net",
        credential=credential
    )
    
    # Get a FileSystemClient object
    filesystem_client = service_client.get_file_system_client(filesystem_name)
    
    # Get a DataLakeFileClient object for the CSV file
    file_client = filesystem_client.get_file_client(directory_path)
    
    
    # Download the file
    download = file_client.download_file()
    downloaded_bytes = download.readall()
    
    # Convert the downloaded bytes to a string
    csv_data = downloaded_bytes.decode('utf-8')
    
    # Use StringIO to read the CSV string into a Pandas DataFrame
    df = pd.read_csv(StringIO(csv_data))
    
    """
    file_data   = file_client.query_file("SELECT * FROM BlobStorage")
    csv_content = file_data.readall().decode('utf-8')
    df = pd.read_csv(StringIO(csv_content), header=0)
    """
    return df

def main(datatype,datasetid):
    
    conn_prop = connection_prop()
    
    ##Chage Between Price/Generation and DatasetID
    output_df = generation_data_output(datasetid,datatype)
    # output_df = generation_data_output(8750,'pricedata')
    
    # datasetdetails_query    =   "Select * from dataset_adls(nolock) where datasetid = "+str(datasetid)
    # datasetdetails          =   pd.read_sql(datasetdetails_query, conn_prop.engine)
    
    
    
    
    if datatype == 'pricedata':
        # pricecontext_query      = 	"Select * from pricecontext(nolock)"
        # pricecontext_details    =   pd.read_sql(pricecontext_query, conn_prop.engine)
        
        output_df['pricecontextid']  = 0
        
        output_df['pricecontextid'] = output_df['pricecontextid'].astype(str)
        # pricecontext_details['pricecontextid'] = pricecontext_details['pricecontextid'].astype(str)
        
        final_output = output_df.copy()
    
    if datatype == 'generationdata':
        powersource_query      = 	"Select * from powersource_mapping_datalake(nolock)"
        powersource_details    =   pd.read_sql(powersource_query, conn_prop.engine)
        final_output = output_df.rename(columns=dict(zip(powersource_details["db_source_name"], powersource_details["output_source_name"])))

    return final_output
    