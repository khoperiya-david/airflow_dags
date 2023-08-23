/*set as deleted*/
with x as
(
    select pr."objectId" as object_id
    from stg.sol src
    cross join lateral jsonb_to_recordset(data) as pr
    (
        "objectId" int
    )
    where entity_name = 'product'
)
update public.sol_products pr
set deleted_at = now() at time zone 'utc'
  , updated_at = now() at time zone 'utc'
where not exists
(
    select 1
    from x
    where x.object_id = pr.object_id
) and deleted_at is null;
/*insert and update*/
with x as
(
    select pr.id
         , pr."objectId" as object_id
         , pr."parentId" as parent_object_id
         , pr.code
         , pr.name
         , pr."rmsContour" as customer_unit_id
         , pr."techTribe" as executor_unit_id
         , pr.owner as owner_login
         , pr."ownerName" as owner
         , pr."techOwner" as tech_owner_login
         , pr."techOwnerName" as tech_owner
         , pr.status
         , pr.description
         , pr."shortDescription" as short_description
         , pr."itObjectStatus" as it_object_status
         , pr.sol_type as type
         , pr.sol_url || '/modules/' || pr.id  as sol_url
         , split_part ( pr.code, '.',1 )  as product_type_sol
         , CASE upper(split_part ( pr.code, '.',1 ))
             WHEN 'TEC' THEN 'Технопродукты'
             WHEN 'INT' THEN 'Внутренние продукты'
             WHEN 'PLF' THEN 'ИТ-продукты. Платформы'
             WHEN 'PRO' THEN 'ИТ-продукты. Приложения'
             WHEN 'SRV' THEN 'ИТ-продукты. Сервисы'
             ELSE 'Внешние продукты (из PPInfo)'
           END AS product_type_sol_name
         , replace(hu.tribe_id,'None','0')::int as tribe_id
    from stg.sol src
    cross join lateral jsonb_to_recordset(data) as pr
    (
        id int
      , "objectId" int
      , "parentId" int
      , code text
      , name text
      , "rmsContour" text
      , "techTribe" text
      , owner text
      , "ownerName" text
      , "techOwnerName" text
      , "status" text
      , "techOwner" text
      , "description" text
      , "shortDescription" text
      , "itObjectStatus" text
      , sol_type text
      , "sol_url" text
    )
    left join public.dwh_hrgate_units hu
        on pr."rmsContour"::text = hu.id
    where entity_name = 'product'
)
insert into public.sol_products
(
    id
  , object_id
  , code
  , name
  , tribe_id
  , customer_unit_id
  , executor_unit_id
  , type
  , owner
  , tech_owner
  , tech_owner_login
  , owner_login
  , product_type_sol
  , product_type_sol_name
  , status
  , description
  , short_description
  , sol_url
  , it_object_status
)
select id
     , object_id
     , code
     , name
     , tribe_id
     , customer_unit_id
     , executor_unit_id
     , type
     , owner
     , tech_owner
     , tech_owner_login
     , owner_login
     , product_type_sol
     , product_type_sol_name
     , status
     , description
     , short_description
     , sol_url
     , it_object_status
from x on conflict (object_id) do update
set deleted_at = null
  , updated_at = now() at time zone 'utc'
  , code = excluded.code
  , name = excluded.name
  , tribe_id = excluded.tribe_id
  , customer_unit_id = excluded.customer_unit_id
  , executor_unit_id = excluded.executor_unit_id
  , type = excluded.type
  , owner = excluded.owner
  , tech_owner = excluded.tech_owner
  , tech_owner_login = excluded.tech_owner_login
  , owner_login = excluded.owner_login
  , product_type_sol = excluded.product_type_sol
  , product_type_sol_name = excluded.product_type_sol_name
  , status = excluded.status
  , description = excluded.description
  , short_description = excluded.short_description
  , sol_url = excluded.sol_url
  , it_object_status = excluded.it_object_status
where
(
     public.sol_products.deleted_at is not null
  or coalesce(public.sol_products.code,'') != coalesce(excluded.code,'')
  or coalesce(public.sol_products.name,'') != coalesce(excluded.name,'')
  or coalesce(public.sol_products.tribe_id,0) != coalesce(excluded.tribe_id,0)
  or coalesce(public.sol_products.customer_unit_id,'') != coalesce(excluded.customer_unit_id,'')
  or coalesce(public.sol_products.executor_unit_id,'') != coalesce(excluded.executor_unit_id,'')
  or coalesce(public.sol_products.type,'') != coalesce(excluded.type,'')
  or coalesce(public.sol_products.owner,'') != coalesce(excluded.owner,'')
  or coalesce(public.sol_products.tech_owner,'') != coalesce(excluded.tech_owner,'')
  or coalesce(public.sol_products.tech_owner_login,'') != coalesce(excluded.tech_owner_login,'')
  or coalesce(public.sol_products.owner_login,'') != coalesce(excluded.owner_login,'')
  or coalesce(public.sol_products.product_type_sol,'') != coalesce(excluded.product_type_sol,'')
  or coalesce(public.sol_products.product_type_sol_name,'') != coalesce(excluded.product_type_sol_name,'')
  or coalesce(public.sol_products.status,'') != coalesce(excluded.status,'')
  or coalesce(public.sol_products.description,'') != coalesce(excluded.description,'')
  or coalesce(public.sol_products.short_description,'') != coalesce(excluded.short_description,'')
  or coalesce(public.sol_products.sol_url,'') != coalesce(excluded.sol_url,'')
  or coalesce(public.sol_products.it_object_status,'') != coalesce(excluded.it_object_status,'')
);