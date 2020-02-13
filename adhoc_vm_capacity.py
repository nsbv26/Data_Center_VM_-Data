import psycopg2
import pandas as pd
import numpy as np
import pymssql
import sys
from pathlib import Path

## Append the the path to the custom modules i.e. CernDBConnector
sys.path.append("C://Users/nb044705/OneDrive - Cerner Corporation/DEVELOPMENT/github")

## Set variable to location of database.ini file
## https://vault.cerner.com/credential/read?credID=680228
CernDBConnector_INI = ("C:/Users/nb044705/OneDrive - Cerner Corporation/development/credentials/database.ini")
from CernDBConnector import config

SQLPath = ("C:/Users/NB044705/OneDrive - Cerner Corporation/development/github/VMData/")


## MSSQL DB Connection
## Obtain the db connection parameters and pass to MS SQL
## module returning a connection to the database
def connectMSSQL(db,CernDBConnector_INI):
    params = config.config(db,CernDBConnector_INI)
    conn = pymssql.connect(**params)
    return(conn)


## Pass in the database to connect to and the sql via a file handle
## return the queried data as a pandas dataframe object
def getMSDBData(db,sql,):
    conn = None
    try:

        ## Open database connection
        conn = connectMSSQL(db,CernDBConnector_INI)

        ## Open and read the file as a single buffer
        sqlFile  = open(SQLPath + 'SQL/' + sql,'r')

        df = pd.read_sql_query(sqlFile.read(),conn)

        ## close db conn and sql file
        sqlFile.close()
        #cur.close()
        return(df)

    except (Exception, pymssql.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

## PostgreSQL DB Connection
## Obtain the db connection parameters and pass to Postgres
## module returning a connection to the database
def connectPGSQL(db,CernDBConnector_INI):
    params = config.config(db,CernDBConnector_INI)
    conn = psycopg2.connect(**params)
    return(conn)


## Pass in the database to connect to and the sql via a file handle
## return the queried data as a pandas dataframe object
def getDBData(db,sql):
    conn = None
    try:

        ## Open database connection
        conn = connectPGSQL(db,CernDBConnector_INI)

        ## Open and read the file as a single buffer
        sqlFile  = open(SQLPath + 'SQL/' + sql,'r')

        df = pd.read_sql_query(sqlFile.read(),conn)

        ## close db conn and sql file
        sqlFile.close()
        #cur.close()
        return(df)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


#######################################################
## VM Summary for Servers in the ESXA and ESXB Pools ##
#######################################################

## get vm data from vcenter
vm_data = getMSDBData('vcenter','adhoc_vm_data.sql')

vm_data = vm_data.groupby(['Environment','vCenter','Cluster'])['VMMemoryGB','VMCDSSpaceGB','VMvCPU'].sum().reset_index()









## write df's to csv
vm_data.to_csv(r'C:/Users/nb044705/Cerner Corporation/SSE IPA Capacity Management - Reference Documents/misc/cluster_vm_data.csv',index=False)

#print(sum_data)

vm_data.head()

