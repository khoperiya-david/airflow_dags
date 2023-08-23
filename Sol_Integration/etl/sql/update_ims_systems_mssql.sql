SET XACT_ABORT, NOCOUNT ON
BEGIN TRY
    BEGIN TRANSACTION;
with x as 
(
    select ImsSystems.object_id as ObjectId
         , ImsSystems.system_id as SystemId
         , ImsSystems.name as Name 
         , ImsSystems.class_name as ClassName
         , ImsSystems.owner_login as OwnerLogin
         , ImsSystems.owner_name as [Owner]
         , ImsSystems.status as [Status]
         , ImsSystems.real_servers as RealServers 
         , ImsSystems.virtual_servers as VirtualServers
         , ImsSystems.person_login as PersonLogin 
         , ImsSystems.person_name as Person 
         , ImsSystems.url as Url 
         , ImsSystems.is_test as IsTest
    from stg.SOL_Json as data 
    cross apply openjson(JsonText)
    with 
    (
           object_id int 'strict $.object_id'
         , system_id int 'strict $.system_id'
         , name nvarchar(400)
         , class_name nvarchar(100)
         , owner_login nvarchar(64)
         , owner_name nvarchar(400)
         , status nvarchar(155)
         , real_servers int 
         , virtual_servers int 
         , person_login nvarchar(64)
         , person_name nvarchar(400)
         , url nvarchar(512)
         , is_test bit
    ) as ImsSystems
    where Entity = 'ims_systems'
),
stg_SOL_SystemsInfo as 
(
    select x.ObjectId
        , x.SystemId
        , x.Name 
        , x.ClassName
        , x.OwnerLogin
        , x.[Owner]
        , x.[Status]
        , x.RealServers 
        , x.VirtualServers
        , x.PersonLogin 
        , x.Person
        , x.Url 
        , x.IsTest
    from x
)
    MERGE INTO [dbo].[SOL_SystemsInfo] AS target
    USING
    (
        SELECT [ObjectId]
             , [SystemId]
             , isnull([Name],'') as [Name]
             , isnull([ClassName],'') as [ClassName]
             , isnull([Owner],'') as [Owner]
             , isnull(OwnerLogin,'') as OwnerLogin
             , isnull([Status],'') as [Status]
             , isnull([RealServers],0) as [RealServers]
             , isnull([VirtualServers],0) as [VirtualServers]
             , isnull([Person],'') as [Person]
             , isnull(PersonLogin,'') as PersonLogin
             , isnull([Url],'') as [Url]
             , isnull(IsTest,0) as IsTest
        FROM stg_SOL_SystemsInfo
    ) AS source
    ON target.ObjectId = source.ObjectId
       AND target.[SystemId] = source.[SystemId]
    WHEN matched AND (target.IS_DELETED = 1
                     OR target.DELETED_AT is not null
                     OR target.[Name] <> source.[Name]
                     OR target.[ClassName] <> source.[ClassName]
                     OR target.[Owner] <> source.[Owner]
                     OR target.[Status] <> source.[Status]
                     OR target.[RealServers] <> source.[RealServers]
                     OR target.[VirtualServers] <> source.[VirtualServers]
                     OR ISNULL (target.[Person], '') <> source.[Person]
                     OR target.[Url] <> source.[Url] 
                     OR ISNULL(target.PersonLogin,'') <> isnull(source.PersonLogin,'')
                     OR ISNULL(target.OwnerLogin,'') <> isnull(source.OwnerLogin,'')
                     OR ISNULL(target.IsTest,0) <> isnull(source.IsTest,0)
    )THEN
        UPDATE SET IS_DELETED = 0
                 , DELETED_AT = NULL
                 , [Name] = source.[Name]
                 , [ClassName] = source.[ClassName]
                 , [Owner] = source.[Owner]
                 , [Status] = source.[Status]
                 , [RealServers] = source.[RealServers]
                 , [VirtualServers] = source.[VirtualServers]
                 , [Person] = source.[Person]
                 , [Url] = source.[Url]
                 , PersonLogin = source.PersonLogin
                 , OwnerLogin = source.OwnerLogin
                 , IsTest = source.IsTest
    WHEN NOT matched BY target THEN
        INSERT
        (
            ObjectId
          , [SystemId]
          , [Name]
          , [ClassName]
          , [Owner]
          , [Status]
          , [RealServers]
          , [VirtualServers]
          , [Person]
          , [Url]
          , PersonLogin
          , OwnerLogin
          , IsTest
        )
        VALUES
        (
            source.ObjectId
          , source.[SystemId]
          , source.[Name]
          , source.[ClassName]
          , source.[Owner]
          , source.[Status]
          , source.[RealServers]
          , source.[VirtualServers]
          , source.[Person]
          , source.[Url]
          , source.PersonLogin
          , source.OwnerLogin
          , source.IsTest
        )
    WHEN NOT matched BY source and IS_DELETED = 0 THEN
        UPDATE SET IS_DELETED = 1
                 , DELETED_AT = SYSUTCDATETIME ();
    COMMIT TRANSACTION
END TRY
BEGIN CATCH
    IF @@trancount > 0
        ROLLBACK TRANSACTION
    DECLARE @msg nvarchar(2048) = ERROR_MESSAGE ()
    RAISERROR (@msg, 16, 1)
END CATCH