SELECT VMUuid as id
      ,[Environment]
      ,[vCenter]
      ,[VMName]
      ,[ESXiHost]
      ,[Cluster]
      ,[VMMemoryMB]/1024 as VMMemoryGB
      ,[VMvCPU]
      ,[ConfigGuestID]
      ,CASE WHEN ConfigGuestID like '%rhel%' THEN 'Linux'
            WHEN ConfigGuestID like '%win%' THEN 'Windows'
            WHEN ConfigGuestID like '%oracle%' THEN 'Linux'
            WHEN ConfigGuestID like '%Linux%' THEN 'Linux'
        END AS os

      
      
  FROM [VMInventory].[dbo].[VMInv]

  where Environment ='RHO'
  and Cluster not like '%Build%'
  and Cluster not like '%RDS%'
  and Cluster not like '%PPM%'
  --and VMState = 'poweredOn'
  and IsActive = 1