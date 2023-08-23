with x as 
(
    select FactTransferId
         , ConversionPeriodId
         , ProjectFrom
         , ProjectTo
         , Coefficient
         , CreatedAt
         , ISNULL(RmsLegalEntityId,0) as RmsLegalEntityId
    from stg.TS_Json s 
    cross apply openjson(JsonText)
    with 
    (
          FactTransferId UNIQUEIDENTIFIER 'strict $.factTransferId'
        , ConversionPeriodId UNIQUEIDENTIFIER '$.conversionPeriodId'
        , ProjectFrom nchar(6) '$.projectFrom'
        , ProjectTo nchar(6) '$.projectTo'
        , Coefficient dec(10,2) '$.coefficient'
        , CreatedAt datetime2 '$.createdAt'
        , RmsLegalEntityId int '$.rmsLegalEntityId'
    ) as data
    where Entity = 'ts_fact_transfers'
)
merge into [dbo].[TS_FactTransfers] as target 
using
(
    select FactTransferId
         , ConversionPeriodId
         , ProjectFrom
         , ProjectTo
         , Coefficient
         , CreatedAt
         , RmsLegalEntityId
    from x
) as source 
on source.FactTransferId = target.FactTransferId
when matched and 
(
    target.IS_DELETED = 1
 or target.DELETED_AT is not null
 or source.ConversionPeriodId <> target.ConversionPeriodId
 or source.ProjectFrom <> target.ProjectFrom
 or source.ProjectTo <> target.ProjectTo
 or source.Coefficient <> target.Coefficient
 or source.CreatedAt <> target.CreatedAt
 or source.RmsLegalEntityId <> target.RmsLegalEntityId 
)
then update 
set DELETED_AT = null
  , IS_DELETED = 0
  , UPDATED_AT = sysutcdatetime()
  , ConversionPeriodId = source.ConversionPeriodId
  , ProjectFrom = source.ProjectFrom
  , ProjectTo = source.ProjectTo
  , Coefficient = source.Coefficient
  , CreatedAt = source.CreatedAt
  , RmsLegalEntityId = source.RmsLegalEntityId
when not matched by target then insert
(
  FactTransferId
, ConversionPeriodId
, ProjectFrom
, ProjectTo
, Coefficient
, CreatedAt
, RmsLegalEntityId    
)
values 
(
  source.FactTransferId
, source.ConversionPeriodId
, source.ProjectFrom
, source.ProjectTo
, source.Coefficient
, source.CreatedAt
, source.RmsLegalEntityId     
)
when not matched by source and IS_DELETED = 0 then update 
set DELETED_AT = sysutcdatetime()
  , IS_DELETED = 1
  , UPDATED_AT = sysutcdatetime();
