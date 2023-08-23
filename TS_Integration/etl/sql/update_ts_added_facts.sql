with x as 
(
    select AddedFactId               
         , ConversionPeriodId        
         , PersonId                  
         , ProjectTo                 
         , AllocationDuration        
         , CreatedAt                 
         , ConversionTimeAllocationId
    from stg.TS_Json s 
    cross apply openjson(JsonText)
    with 
    (
          AddedFactId                uniqueidentifier 'strict $.addedFactId'
        , ConversionPeriodId         uniqueidentifier '$.conversionPeriodId'
        , PersonId                   nchar(7)         '$.personId'
        , ProjectTo                  nchar(6)         '$.projectTo'
        , AllocationDuration         dec(10, 2)       '$.allocationDuration'
        , CreatedAt                  datetime2        '$.createdAt'
        , ConversionTimeAllocationId uniqueidentifier '$.conversionTimeAllocationId'
    ) as data
    where Entity = 'ts_added_facts'
)
merge into [dbo].[TS_AddedFacts] as target 
using
(
    select AddedFactId               
         , ConversionPeriodId        
         , PersonId                  
         , ProjectTo                 
         , AllocationDuration        
         , CreatedAt                 
         , ConversionTimeAllocationId
    from x
) as source 
on source.AddedFactId = target.AddedFactId
when matched and 
(
    target.IS_DELETED = 1
 or target.DELETED_AT is not null
 or source.ConversionPeriodId <> target.ConversionPeriodId
 or source.PersonId <> target.PersonId
 or source.ProjectTo <> target.ProjectTo
 or source.AllocationDuration <> target.AllocationDuration
 or source.CreatedAt <> target.CreatedAt
 or source.ConversionTimeAllocationId <> target.ConversionTimeAllocationId 
)
then update 
set DELETED_AT = null
  , IS_DELETED = 0
  , UPDATED_AT = sysutcdatetime()
  , ConversionPeriodId = source.ConversionPeriodId
  , PersonId = source.PersonId
  , ProjectTo = source.ProjectTo
  , AllocationDuration = source.AllocationDuration
  , CreatedAt = source.CreatedAt
  , ConversionTimeAllocationId = source.ConversionTimeAllocationId
when not matched by target then insert
(
  AddedFactId
, ConversionPeriodId
, PersonId
, ProjectTo
, AllocationDuration
, CreatedAt
, ConversionTimeAllocationId    
)
values 
(
  source.AddedFactId
, source.ConversionPeriodId
, source.PersonId
, source.ProjectTo
, source.AllocationDuration
, source.CreatedAt
, source.ConversionTimeAllocationId     
)
when not matched by source and IS_DELETED = 0 then update 
set DELETED_AT = sysutcdatetime()
  , IS_DELETED = 1
  , UPDATED_AT = sysutcdatetime();
