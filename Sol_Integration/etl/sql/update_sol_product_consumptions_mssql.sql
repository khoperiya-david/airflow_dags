with stg_consumptions as 
(
    SELECT data.SOURCE_SYSTEM_ID
         , isnull(data.TYPE,'') as TYPE
         , isnull(data.CONSUMER_OBJECT_ID,0) as CONSUMER_OBJECT_ID
         , isnull(data.FOOD_PRODUCT_OBJECT_ID,0) as FOOD_PRODUCT_OBJECT_ID
         , isnull(data.LINK_TYPE,'') as LINK_TYPE
         , isnull(data.LINK_WEIGHT,0.0) as LINK_WEIGHT
         , isnull(data.INTEGRATION_STAGE,'') as INTEGRATION_STAGE
         , isnull(data.CONFIRMED_IN_OBS,0) as CONFIRMED_IN_OBS
         , isnull(data.CREATION_DATE,'0001-01-01') as CREATION_DATE
         , isnull(data.LAST_CHANGE_DATE,'0001-01-01') as LAST_CHANGE_DATE
         , isnull(data.STATUS,'') as STATUS
         , isnull(data.COMMENT,'') as COMMENT
    FROM stg.SOL_Json
    CROSS APPLY OPENJSON (JsonText)
    WITH
    (
          SOURCE_SYSTEM_ID int 'strict $.id'
        , TYPE NVARCHAR(64) '$.type'
        , CONSUMER_OBJECT_ID bigint '$.consumer_product_id'
        , FOOD_PRODUCT_OBJECT_ID bigint '$.food_product_module_object_id'
        , LINK_TYPE nvarchar(64) '$.link_type'
        , LINK_WEIGHT decimal(19,4) '$.link_weight'
        , INTEGRATION_STAGE nvarchar(64) '$.integration_stage'
        , CONFIRMED_IN_OBS bit '$.confirmed_in_obs'
        , CREATION_DATE datetime2 '$.creation_date'
        , LAST_CHANGE_DATE datetime2 '$.last_change_date'
        , STATUS nvarchar(64) '$.status'
        , COMMENT nvarchar(1024) '$.comment'
    ) AS data
    where entity = 'consumptions'  
)
merge into [dbo].[SOL_ProductConsumptions] as target 
using 
(
    select SOURCE_SYSTEM_ID
         , TYPE
         , CONSUMER_OBJECT_ID
         , FOOD_PRODUCT_OBJECT_ID
         , LINK_TYPE
         , LINK_WEIGHT
         , INTEGRATION_STAGE
         , CONFIRMED_IN_OBS
         , CREATION_DATE
         , LAST_CHANGE_DATE
         , STATUS
         , COMMENT
    from stg_consumptions
) as source 
on source.SOURCE_SYSTEM_ID = target.SOURCE_SYSTEM_ID
when matched and 
(
    target.IS_DELETED = 1
 or target.DELETED_AT is not null 
 or source.TYPE <> target.TYPE
 or source.CONSUMER_OBJECT_ID <> target.CONSUMER_OBJECT_ID
 or source.FOOD_PRODUCT_OBJECT_ID <> target.FOOD_PRODUCT_OBJECT_ID
 or source.LINK_TYPE <> target.LINK_TYPE
 or source.LINK_WEIGHT <> target.LINK_WEIGHT
 or source.INTEGRATION_STAGE <> target.INTEGRATION_STAGE
 or source.CONFIRMED_IN_OBS <> target.CONFIRMED_IN_OBS
 or source.CREATION_DATE <> target.CREATION_DATE
 or source.LAST_CHANGE_DATE <> target.LAST_CHANGE_DATE
 or source.STATUS <> target.STATUS
 or source.COMMENT <> target.COMMENT   
)
then update 
set DELETED_AT = NULL
  , IS_DELETED = 0
  , TYPE = source.TYPE
  , CONSUMER_OBJECT_ID = source.CONSUMER_OBJECT_ID
  , FOOD_PRODUCT_OBJECT_ID = source.FOOD_PRODUCT_OBJECT_ID
  , LINK_TYPE = source.LINK_TYPE
  , LINK_WEIGHT = source.LINK_WEIGHT
  , INTEGRATION_STAGE = source.INTEGRATION_STAGE
  , CONFIRMED_IN_OBS = source.CONFIRMED_IN_OBS
  , CREATION_DATE = source.CREATION_DATE
  , LAST_CHANGE_DATE = source.LAST_CHANGE_DATE
  , STATUS = source.STATUS
  , COMMENT = source.COMMENT
when not matched by target then insert 
(
      SOURCE_SYSTEM_ID
    , TYPE
    , CONSUMER_OBJECT_ID
    , FOOD_PRODUCT_OBJECT_ID
    , LINK_TYPE
    , LINK_WEIGHT
    , INTEGRATION_STAGE
    , CONFIRMED_IN_OBS
    , CREATION_DATE
    , LAST_CHANGE_DATE
    , STATUS
    , COMMENT
)
VALUES 
(
      source.SOURCE_SYSTEM_ID
    , source.TYPE
    , source.CONSUMER_OBJECT_ID
    , source.FOOD_PRODUCT_OBJECT_ID
    , source.LINK_TYPE
    , source.LINK_WEIGHT
    , source.INTEGRATION_STAGE
    , source.CONFIRMED_IN_OBS
    , source.CREATION_DATE
    , source.LAST_CHANGE_DATE
    , source.STATUS
    , source.COMMENT
)
when not matched by source and target.IS_DELETED = 0 then update 
set IS_DELETED = 1
  , DELETED_AT = SYSUTCDATETIME();