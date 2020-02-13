select
cmdb_ci_nme as name
,cmdb_os
,CASE WHEN cmdb_os like '%Linux%' THEN 'Linux'
    WHEN cmdb_os like '%Window%' THEN 'Windows'
    
END AS rem_os

From cfg_cmdb_compute