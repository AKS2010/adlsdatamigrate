# -*- coding: utf-8 -*-
"""
Created on Tue Aug  6 19:06:35 2024

@author: AshokKumarSathasivam
"""
import os
import json
from azure.storage.blob import BlobServiceClient
import sqlalchemy
import urllib



class connection_prop:
    def __init__(self):

        self.container_name = "dataset"

        ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
        config_path = '\\'.join([ROOT_DIR, 'config.json'])

        # read json file
        with open(config_path) as config_file:
            config = json.load(config_file)
            sql_config = config['database']
            sa_config  = config['storage_account_key']
            dl_config  = config['datalake_api_key']

        # =============================================================================
        #         DATABASE CONNECTION PROPERTIES
        # =============================================================================

        self.SQL_server = sql_config['SQL_server']
        self.SQL_database = sql_config['SQL_database']
        self.SQL_user = sql_config['SQL_user']
        self.SQL_password = sql_config['SQL_password']
        self.driver = sql_config['driver']

        self.params = 'DRIVER='+self.driver + ';SERVER='+self.SQL_server + ';PORT=1433;DATABASE=' + \
            self.SQL_database + ';UID=' + self.SQL_user + \
            ';PWD=' + self.SQL_password + ';Mars_Connection=Yes'
        self.db_params = urllib.parse.quote_plus(self.params)

        # =============================================================================
        #         BLOB STORAGE CONNECTION PROPERTIES
        # =============================================================================

        self.storage_account_name = sa_config['account_name']
        self.storage_account_key = sa_config['account_key']

        self.connection_string = f"DefaultEndpointsProtocol=https;AccountName={self.storage_account_name};AccountKey={self.storage_account_key};EndpointSuffix=core.windows.net"

        self.engine = sqlalchemy.create_engine(
            "mssql+pyodbc:///?odbc_connect={}".format(self.db_params), fast_executemany=True)
        self.blob_service_client = BlobServiceClient.from_connection_string(
            self.connection_string)
        self.pyodbc_connection = self.engine.raw_connection()
        
        # =============================================================================
        #         ADLS STORAGE CONNECTION PROPERTIES
        # =============================================================================
        
        self.tenant_id              = dl_config['tenant_id']
        self.client_id              = dl_config['client_id']
        self.client_secret          = dl_config['client_secret']
        self.storage_account_name   = dl_config['storage_account_name']
        self.filesystem_name        = dl_config['filesystem_name']
        
        self.storage_account_name   = dl_config['storage_account_name']
        self.filesystem_name        = dl_config['filesystem_name']
