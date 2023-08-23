/*set as deleted*/
with x as
(
    select systems.object_id
         , systems.system_id
    from stg.sol src
    cross join lateral jsonb_to_recordset(data) as systems
    (
               object_id int
             , system_id int
    )
    where entity_name = 'ims_systems'
)
update public.sol_imssystems ims
set deleted_at = now() at time zone 'utc'
  , updated_at = now() at time zone 'utc'
where not exists
(
    select 1
    from x
    where x.object_id = ims.object_id
      and x.system_id = ims.system_id
) and deleted_at is null;
/*insert and update*/
with x as
(
    select systems.object_id
         , systems.system_id
         , systems.name
         , systems.class_name
         , systems.owner_login
         , systems.owner_name
         , systems.status
         , systems.real_servers
         , systems.virtual_servers
         , systems.person_login
         , systems.person_name
         , systems.url
         , systems.is_test
    from stg.sol src
    cross join lateral jsonb_to_recordset(data) as systems
    (
           object_id int
         , system_id int
         , name text
         , class_name text
         , owner_login text
         , owner_name text
         , status text
         , real_servers int
         , virtual_servers int
         , person_login text
         , person_name text
         , url text
         , is_test bool
    )
    where entity_name = 'ims_systems'
)
insert into public.sol_imssystems
(
    object_id
  , system_id
  , name
  , classname
  , status
  , url
  , istest
  , owner
  , owner_login
  , person
  , person_login
  , real_servers
  , virtual_servers
)
select object_id
     , system_id
     , name
     , class_name as classname
     , status
     , url
     , is_test as istest
     , owner_name as owner
     , owner_login
     , person_name as person
     , person_login
     , real_servers
     , virtual_servers
from x
on conflict (object_id, system_id) do update
set name = excluded.name
  , classname = excluded.classname
  , status = excluded.status
  , url = excluded.url
  , istest = excluded.istest
  , owner = excluded.owner
  , owner_login = excluded.owner_login
  , person = excluded.person
  , person_login = excluded.person_login
  , real_servers = excluded.real_servers
  , virtual_servers = excluded.virtual_servers
  , deleted_at = null
  , updated_at = now() at time zone 'utc'
where
(
    public.sol_imssystems.deleted_at is not null
 or coalesce(public.sol_imssystems.name,'') <> coalesce(excluded.name,'')
 or coalesce(public.sol_imssystems.classname,'') <> coalesce(excluded.classname,'')
 or coalesce(public.sol_imssystems.status,'') <> coalesce(excluded.status,'')
 or coalesce(public.sol_imssystems.url,'') <> coalesce(excluded.url,'')
 or coalesce(public.sol_imssystems.istest,false) <> coalesce(excluded.istest,false)
 or coalesce(public.sol_imssystems.owner,'') <> coalesce(excluded.owner,'')
 or coalesce(public.sol_imssystems.owner_login,'') <> coalesce(excluded.owner_login,'')
 or coalesce(public.sol_imssystems.person,'') <> coalesce(excluded.person,'')
 or coalesce(public.sol_imssystems.person_login,'') <> coalesce(excluded.person_login,'')
 or coalesce(public.sol_imssystems.real_servers,0) <> coalesce(excluded.real_servers,0)
 or coalesce(public.sol_imssystems.virtual_servers,0) <> coalesce(excluded.virtual_servers,0)
) ;