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

## get server os data from Remedy
rem_data = getDBData('cmis','rem_data.sql')
rem_data['name'] = rem_data['name'].str.lower()

## split fq name to name and domain
new = rem_data["name"].str.split(".", n = 1, expand = True)
  ## Making separate name column from new data frame 
rem_data["vmname"]= new[0]
  ## Making separate domain column from new data frame 
rem_data["domain"]= new[1]

## get vm data from vcenter
vm_data = getMSDBData('vcenter','vm_data.sql')
vm_data['location'] = vm_data['ESXiHost'].map(lambda x: x[0:3])
vm_data['type'] = vm_data['ESXiHost'].map(lambda x: x[3:7])
vm_data.rename(columns = {'VMName' : 'vmname'}, inplace = True)
vm_data['vmname'] = vm_data['vmname'].str.lower()
vm_data.drop_duplicates(subset ="id",keep = False, inplace = True)  

## add sql clusters
sqlvm_data = getMSDBData('vcenter','vm_data.sql')
sqlvm_data['location'] = vm_data['ESXiHost'].map(lambda x: x[0:3])
sqlvm_data['type'] = vm_data['ESXiHost'].map(lambda x: x[3:9])
sqlvm_data.rename(columns = {'VMName' : 'vmname'}, inplace = True)
sqlvm_data['vmname'] = vm_data['vmname'].str.lower()
sqlvm_data.drop_duplicates(subset ="id",keep = False, inplace = True)
sqlvm_data = pd.merge(sqlvm_data,rem_data, on='vmname',how='left')

## merge vcenter data and remedy data
vm_data = pd.merge(vm_data,rem_data, on='vmname',how='left')

## fill in missing os from vc with remedy data
vm_data.os.fillna(vm_data.rem_os, inplace=True)

## fill blank os with other
vm_data.os.fillna('Other', inplace=True)

## prod mt servers
esxa = vm_data['type']=='esxa'
esxa = vm_data[esxa]
esxa = esxa.reset_index(drop=True)
prod = esxa.groupby(['location','type','os'])['VMMemoryGB'].sum().reset_index()

## non-prod mt servers
esxb = vm_data['type']=='esxb'
esxb = vm_data[esxb]
esxb = esxb.reset_index(drop=True)
nonprod = esxb.groupby(['location','type','os'])['VMMemoryGB'].sum().reset_index()

## sql mt servers
esxsql = sqlvm_data['type']=='esxsql'
esxsql = sqlvm_data[esxsql]
esxsql = esxsql.reset_index(drop=True)
sql = esxsql.groupby(['location','type','os'])['VMMemoryGB'].sum().reset_index()

## vm data from the esxa, esxb, and esxsql pools
vm_data = [esxa,esxb,esxsql]
vm_data = pd.concat(vm_data)
vm_data = vm_data.reset_index(drop=True)

## merge the prod, nonprod, and sql df's
full_data = [prod,nonprod,sql]
full_data = pd.concat(full_data)
sum_data = full_data.reset_index(drop=True)

## get esx quantity
sum_data['esx_qty'] = sum_data['VMMemoryGB']/690

sum_data = sum_data.round()


##########################################################
## Host and Cluster Summary for the ESXA and ESXB Pools ##
##########################################################

## get host data
host_data = getMSDBData('vcenter','host_data.sql')
sqlhost_data = getMSDBData('vcenter','host_data.sql')

## Extract region, dc, and pool type from the hostname
host_data['region'] = host_data['HostName'].map(lambda x: x[0:2])
host_data['location'] = host_data['HostName'].map(lambda x: x[0:3])
host_data['type'] = host_data['HostName'].map(lambda x: x[3:7])

sqlhost_data['region'] = host_data['HostName'].map(lambda x: x[0:2])
sqlhost_data['location'] = host_data['HostName'].map(lambda x: x[0:3])
sqlhost_data['type'] = host_data['HostName'].map(lambda x: x[3:9])

## Change region from KC and LS to US
host_data['region']= host_data['region'].replace('kc', 'us') 
host_data['region']= host_data['region'].replace('ls', 'us')

sqlhost_data['region']= host_data['region'].replace('kc', 'us') 
sqlhost_data['region']= host_data['region'].replace('ls', 'us')

## Use the US region to filter out global hosts
ushost_data = host_data['region']=='us'
host_data = host_data[ushost_data]
host_data = host_data.reset_index(drop=True) 

sqlushost_data = sqlhost_data['region']=='us'
sqlhost_data = sqlhost_data[ushost_data]
sqlhost_data = sqlhost_data.reset_index(drop=True) 

## esxa hosts
esxa_cluster = host_data['type']=='esxa'
hesxa = host_data[esxa_cluster]
hesxa = hesxa.reset_index(drop=True)

## esxb hosts
esxb_cluster = host_data['type']=='esxb'
hesxb = host_data[esxb_cluster]
hesxb = hesxb.reset_index(drop=True)

## esxsql hosts
esxsql_cluster = sqlhost_data['type']=='esxsql'
hesxsql = sqlhost_data[esxb_cluster]
hesxsql = hesxsql.reset_index(drop=True)

## host data
host_data = [hesxa,hesxb,hesxsql]
host_data = pd.concat(host_data, sort=False)
host_data = host_data.reset_index(drop=True)

## host memory total
memory = host_data.groupby(['location','Model','CPUModel'])['MemorySize'].sum().reset_index()

## cluster summary
cluster_summary = host_data.groupby(['location','Cluster','Model','CPUModel'])['HostName'].count().reset_index()
cluster_summary.rename(columns = {'HostName' : 'count'}, inplace = True)
cluster_summary['count'].apply(str)
cluster_summary['count'] = cluster_summary['count'].apply(str)
cluster_summary['cluster_count'] = cluster_summary['Cluster'] + " " + cluster_summary['count']
cluster_summary = cluster_summary.groupby(['location','Model','CPUModel'])['cluster_count'].apply(' | '.join).reset_index()

## dc host summary
host_summary = host_data.groupby(['location','Model','CPUModel'])['HostName'].count().reset_index()
host_summary.rename(columns = {'HostName' : 'count'}, inplace = True)
host_summary = pd.merge(host_summary, cluster_summary, on=['location','Model','CPUModel'], how='left')
host_summary = pd.merge(host_summary, memory, on=['location','Model','CPUModel'], how='left')
host_summary['usable_memory'] = host_summary['MemorySize']*.9
host_summary.rename(columns = {'MemorySize' : 'installed_memory'}, inplace = True)
host_summary.rename(columns = {'Model' : 'model'}, inplace = True)
host_summary.rename(columns = {'CPUModel' : 'cpu_model'}, inplace = True)


## write df's to csv
vm_data.to_csv(r'C:/Users/nb044705/Cerner Corporation/SSE IPA Capacity Management - Reference Documents/misc/full_vm_data.csv',index=False)
sum_data.to_csv(r'C:/Users/nb044705/Cerner Corporation/SSE IPA Capacity Management - Reference Documents/misc/vm_data.csv',index=False)
host_summary.to_csv(r'C:/Users/nb044705/Cerner Corporation/SSE IPA Capacity Management - Reference Documents/misc/host_summary.csv',index=False)

print(hesxsql)

#sum_data.head()

