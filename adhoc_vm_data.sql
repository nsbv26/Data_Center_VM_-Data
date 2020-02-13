SELECT
      [Environment]
      ,[vCenter]
      ,[Cluster]
      ,[VMCDSSpaceGB]
      ,[VMMemoryMB]/1024 as VMMemoryGB
      ,[VMvCPU]

      
  FROM [VMInventory].[dbo].[VMInv]

  where IsActive = 1

