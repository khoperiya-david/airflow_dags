SET XACT_ABORT, NOCOUNT ON
BEGIN TRY
    BEGIN TRANSACTION;

    with classifiers as 
    (
        select clsf.id as Id
            , case clsf.clf_id
                when 8 then 'FINDirectionGeneral'
                when 9 then 'FINDirectionDetailed'
                when 27 then 'FINGroup'
                when 28 then 'FINDetailed'
            end as Classifier 
            , clsf.classifier_value_name as ClassifierValue
        from 
        stg.SOL_Json data 
        cross apply openjson(JsonText)
        with
        (
            id int,
            clf_id int,
            classifier_value_name NVARCHAR(255)
        ) as clsf
        where Entity = 'classifiers'
    ),
    classifiers_pvt as 
    (
    select Id 
            , FINDirectionGeneral
            , FINDirectionDetailed
            , FINGroup
            , FINDetailed
    from 
    ( 
            select [Id]
                , [Classifier]
                , [ClassifierValue]
            from classifiers
            where Classifier in ('FINDirectionGeneral','FINDirectionDetailed','FINGroup','FINDetailed')
    ) as p 
    pivot 
    (
            MAX(ClassifierValue) 
            FOR Classifier 
            in ("FINDirectionGeneral","FINDirectionDetailed","FINGroup","FINDetailed")
    ) as pvt
    )
    ,products
    AS 
    (
        SELECT data.Id
             , data.ObjectId
             , ParentId
             , Code
             , Name
             , TribeUnitId
             , OwnerLogin
             , TechOwnerLogin
             , status
             , Description
             , ShortDescription
             , ItObjectStatus
             , sol_type AS [Type]
             , sol_url as SolUrl
        FROM stg.SOL_Json
            CROSS apply
            OPENJSON (JsonText)
            WITH
            (
              Id int 'strict $.id'
            , ObjectId int 'strict $.objectId'
            , ParentId int '$.parentId'
            , Code nvarchar (32) '$.code'
            , Name nvarchar (512) '$.name'
            , TribeUnitId uniqueidentifier '$.rmsContour'
            , OwnerLogin nvarchar (100) '$.owner'
            , TechOwnerLogin nvarchar (100) '$.techOwner'
            , status nvarchar (256) '$.status'
            , Description NVARCHAR(4000) '$.description'
            , ShortDescription NVARCHAR(512) '$.shortDescription'
            , ItObjectStatus nvarchar (255) '$.itObjectStatus'
            , sol_type nvarchar (64)
            , sol_url nvarchar (255)
            ) AS data
        where entity = 'product'  
    ),
    stg_products as
    (
        SELECT x.ObjectId
             , isnull(px.ObjectID,0) AS ParentObjectId
             , x.Code
             , x.Name
             , isnull(mu.TribeCode,0) as TribeCode
             , isnull(p.PersonCode,'') AS OwnerPersonCode
             , isnull(p1.PersonCode,'') AS TechOwnerPersonCode
             , isnull(cls.FINDirectionGeneral,'') as FINDirectionGeneral
             , isnull(cls.FINDirectionDetailed,'') as FINDirectionDetailed
             , isnull(cls.FINGroup,'') as FINGroup
             , isnull(cls.FINDetailed,'') as FINDetailed
             , CASE WHEN CHARINDEX ('.', x.Code) > 1 THEN LEFT(x.Code, CHARINDEX ('.', x.Code) - 1) ELSE 'BPR' END as ProductTypeSol 
             , CASE (CASE WHEN CHARINDEX ('.', x.Code) > 1 THEN LEFT(x.Code, CHARINDEX ('.', x.Code) - 1) ELSE 'BPR' END)
                 WHEN 'TEC' THEN 'Технопродукты'
                 WHEN 'INT' THEN 'Внутренние продукты'
                 WHEN 'PLF' THEN 'ИТ-продукты. Платформы'
                 WHEN 'PRO' THEN 'ИТ-продукты. Приложения'
                 WHEN 'SRV' THEN 'ИТ-продукты. Сервисы'
                 ELSE 'Внешние продукты (из PPInfo)'
             END AS ProductTypeSolName
             , x.[Type]
             , x.Status
             , concat(x.SolUrl , '/modules/', x.Id ) as SolUrl
             , isnull(x.ShortDescription,'') as ShortDescription
             , isnull(x.Description,'') as Description
             , isnull(x.itObjectStatus,'') as itObjectStatus
        FROM products as x
            LEFT JOIN products AS px
                ON x.ParentId = px.Id
            LEFT JOIN dbo.HRGate_Units_ManagementStructure mu
                ON x.TribeUnitId = mu.UnitId
            LEFT JOIN dbo.HRGate_Employees e
                ON e.[Login] = x.OwnerLogin
                    AND e.DeletedAtSource = '0001-01-01'
                    AND e.IS_DELETED = 0
            LEFT JOIN dbo.viw_HRGate_Persons p
                ON e.PersonId = p.PersonId
            LEFT JOIN dbo.HRGate_Employees e1
                ON e1.[Login] = x.TechOwnerLogin
                    AND e1.DeletedAtSource = '0001-01-01'
                    AND e1.IS_DELETED = 0
            LEFT JOIN dbo.viw_HRGate_Persons p1
                ON e1.PersonId = p1.PersonId
            LEFT JOIN classifiers_pvt cls 
                ON x.Id = cls.Id
    ) 
    MERGE INTO dbo.SOL_Products target
    using
    (
        SELECT sp.[ObjectId]
             , sp.[ParentObjectId]
             , sp.[Code]
             , sp.[Name]
			 , sp.[TribeCode] 
             , sp.[Type]
             , sp.[OwnerPersonCode]
             , sp.[TechOwnerPersonCode]
             , sp.[FINDirectionGeneral]
             , sp.[FINDirectionDetailed]
             , sp.[FINGroup]
             , sp.[FINDetailed]
             , sp.[ProductTypeSol]
             , sp.[ProductTypeSolName]
             , sp.[Status]
             , sp.[Description]
             , sp.[SolUrl]
             , sp.[ShortDescription] 
             , sp.[itObjectStatus]
        FROM stg_products sp
        
    ) AS source
    ON source.ObjectId = target.ObjectId
    WHEN matched AND source.[ParentObjectId] <> target.[ParentObjectId]
                     OR source.[Code] <> target.[Code]
                     OR source.[Name] <> target.[Name]
                     OR source.[TribeCode] <> target.[TribeCode]
                     OR source.[Type] <> target.[Type]
                     OR source.[OwnerPersonCode] <> target.[OwnerPersonCode]
                     OR source.[TechOwnerPersonCode] <> target.[TechOwnerPersonCode]
                     OR source.[FINDirectionGeneral] <> target.[FINDirectionGeneral]
                     OR source.[FINDirectionDetailed] <> target.[FINDirectionDetailed]
                     OR source.[FINGroup] <> target.[FINGroup]
                     OR source.[FINDetailed] <> target.[FINDetailed]
                     OR source.[ProductTypeSol] <> target.[ProductTypeSol]
                     OR source.[ProductTypeSolName] <> target.[ProductTypeSolName]
                     OR source.[Status] <> target.[Status]
                     OR source.[Description] <> target.[Description]
                     OR source.[SolUrl] <> target.[SolUrl]
                     OR ISNULL (source.[ShortDescription], '') <> ISNULL (target.[ShortDescription], '')
                     OR ISNULL (source.[itObjectStatus], '') <> ISNULL (target.[itObjectStatus], '') THEN
        UPDATE SET UpdatedAt = SYSUTCDATETIME ()
                 , [ParentObjectId] = source.[ParentObjectId]
                 , [Code] = source.[Code]
                 , [Name] = source.[Name]
                 , [TribeCode] = source.[TribeCode]
                 , [Type] = source.[Type]
                 , [OwnerPersonCode] = source.[OwnerPersonCode]
                 , [TechOwnerPersonCode] = source.[TechOwnerPersonCode]
                 , [FINDirectionGeneral] = source.[FINDirectionGeneral]
                 , [FINDirectionDetailed] = source.[FINDirectionDetailed]
                 , [FINGroup] = source.[FINGroup]
                 , [FINDetailed] = source.[FINDetailed]
                 , [ProductTypeSol] = source.[ProductTypeSol]
                 , [ProductTypeSolName] = source.[ProductTypeSolName]
                 , [Status] = source.[Status]
                 , [Description] = source.[Description]
                 , [SolUrl] = source.[SolUrl]
                 , [ShortDescription] = source.[ShortDescription]
                 , [itObjectStatus] = source.[itObjectStatus]
    WHEN NOT matched BY target THEN
        INSERT
        (
            [ObjectId]
          , [ParentObjectId]
          , [Code]
          , [Name]
          , [TribeCode]
          , [Type]
          , [OwnerPersonCode]
          , [TechOwnerPersonCode]
          , [FINDirectionGeneral]
          , [FINDirectionDetailed]
          , [FINGroup]
          , [FINDetailed]
          , [ProductTypeSol]
          , [ProductTypeSolName]
          , [Status]
          , [Description]
          , [SolUrl]
          , ShortDescription
          , itObjectStatus
        )
        VALUES
        (source.[ObjectId], source.[ParentObjectId], source.[Code], source.[Name], source.[TribeCode], source.[Type]
       , source.[OwnerPersonCode], source.[TechOwnerPersonCode], source.[FINDirectionGeneral]
       , source.[FINDirectionDetailed], source.[FINGroup], source.[FINDetailed], source.[ProductTypeSol]
       , source.[ProductTypeSolName], source.[Status], source.[Description], source.[SolUrl], source.ShortDescription
       , source.itObjectStatus)
    WHEN NOT matched BY source THEN
        DELETE;
    COMMIT TRANSACTION
END TRY
BEGIN CATCH
    IF @@trancount > 0
        ROLLBACK TRANSACTION
    DECLARE @msg nvarchar(2048) = ERROR_MESSAGE ()
    RAISERROR (@msg, 16, 1)
END CATCH;

EXECUTE [stg].[pr_SOL_Services_Actualization];

