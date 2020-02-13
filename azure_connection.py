# Import the required modules from azure.datalake.store import core, lib

# Define the parameters needed to authenticate using client secret token = lib.auth(tenant_id = 'TENANT',
                 client_secret = 'SECRET',
                 client_id = 'ID')

# Create a filesystem client object for the Azure Data Lake Store name (ADLS) adl = core.AzureDLFileSystem(token, store_name='ADLS Account Name')

''' Please visit here to check the list of operations ADLS filesystem client can perform - (https://azure-datalake-store.readthedocs.io/en/latest/api.html#azure.datalake.store.core.AzureDLFileSystem) '''
