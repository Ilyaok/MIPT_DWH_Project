-- Инкрементальная загрузка данных по клиентам

-- 1. Очистка стейджингов.
delete from demipt2.gold_stg_dim_clients;
delete from demipt2.gold_stg_dim_clients_del;

-- 2. Захват данных в стейджинг (кроме удалений).
insert into demipt2.gold_stg_dim_clients (
    client_id,
    last_name,
    first_name,
    patronymic,
    date_of_birth,
    passport_num,
    passport_valid_to,
    phone,
    create_dt,
    update_dt
)
select
    client_id,
    last_name,
    first_name,
    patronymic,
    date_of_birth,
    passport_num,
    passport_valid_to,
    phone,
    create_dt,
    case
    when update_dt is NULL then create_dt
    else current_date end as update_dt
from bank.clients
where 1=0
    or update_dt > (
    select coalesce( last_update_dt, to_date( '1900-01-01', 'YYYY-MM-DD') )
    from demipt2.gold_meta_bank where table_db = 'bank' and table_name = 'clients' )
    or update_dt is null;


-- 3. Захват ключей для вычисления удалений.
insert into demipt2.gold_stg_dim_clients_del ( client_id )
select client_id from bank.clients;


-- 4. Выделяем "вставки" и "обновления" и вливаем их в приемник

merge into demipt2.gold_dwh_dim_clients_hist tgt
using (
    select
        s.client_id,
        s.last_name,
        s.first_name,
        s.patronymic,
        s.date_of_birth,
        s.passport_num,
        s.passport_valid_to,
        s.phone,
        s.update_dt
    from demipt2.gold_stg_dim_clients s
    left join demipt2.gold_dwh_dim_clients_hist t
    on s.client_id = t.client_id and t.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' ) and deleted_flg = 'N'
    where
      1=1
      and t.client_id is not null
      and (
      1=0
      or ( s.last_name <> t.last_name ) or ( s.last_name is null and t.last_name is not null ) or ( s.last_name is not null and t.last_name is null )
      or ( s.first_name <> t.first_name ) or ( s.first_name is null and t.first_name is not null ) or ( s.first_name is not null and t.first_name is null )
      or ( s.patronymic <> t.patronymic ) or ( s.patronymic is null and t.patronymic is not null ) or ( s.patronymic is not null and t.patronymic is null )
      or ( s.date_of_birth <> t.date_of_birth ) or ( s.date_of_birth is null and t.date_of_birth is not null )
          or ( s.date_of_birth is not null and t.date_of_birth is null )
      or ( s.passport_num <> t.passport_num ) or ( s.passport_num is null and t.passport_num is not null )
          or ( s.passport_num is not null and t.passport_num is null )
      or ( s.passport_valid_to <> t.passport_valid_to ) or ( s.passport_valid_to is null and t.passport_valid_to is not null )
          or ( s.passport_valid_to is not null and t.passport_valid_to is null )
      or ( s.phone <> t.phone ) or ( s.phone is null and t.phone is not null ) or ( s.phone is not null and t.phone is null )
      )
) stg
on ( tgt.client_id = stg.client_id )
when matched then update set effective_to = stg.update_dt - interval '1' second
where tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' );


insert into demipt2.gold_dwh_dim_clients_hist (
    client_id,
    last_name,
    first_name,
    patronymic,
    date_of_birth,
    passport_num,
    passport_valid_to,
    phone,
    effective_from,
	effective_to,
	deleted_flg
)
select
    client_id,
    last_name,
    first_name,
    patronymic,
    date_of_birth,
    passport_num,
    passport_valid_to,
    phone,
	update_dt as effective_from,
	to_date( '9999-12-31', 'YYYY-MM-DD' ) as effective_to,
	'N' as deleted_flg
from demipt2.gold_stg_dim_clients;

-- 5. Удаляем из приемника удаленные записи
insert into demipt2.gold_dwh_dim_clients_hist (
    client_id,
    last_name,
    first_name,
    patronymic,
    date_of_birth,
    passport_num,
    passport_valid_to,
    phone,
    effective_from,
	effective_to,
	deleted_flg)
select
    client_id,
    last_name,
    first_name,
    patronymic,
    date_of_birth,
    passport_num,
    passport_valid_to,
    phone,
	current_date as effective_from,
	to_date( '9999-12-31', 'YYYY-MM-DD' ) as effective_to,
	'Y' as deleted_flg
from (
    select
        t.client_id,
        t.last_name,
        t.first_name,
        t.patronymic,
        t.date_of_birth,
        t.passport_num,
        t.passport_valid_to,
        t.phone
    from demipt2.gold_dwh_dim_clients_hist t
    left join demipt2.gold_stg_dim_clients_del s
    on t.client_id = s.client_id and t.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' ) and deleted_flg = 'N'
    where s.client_id is null);

update demipt2.gold_dwh_dim_clients_hist
set effective_to = current_date - interval '1' second
where client_id in (
    select
        t.client_id
    from demipt2.gold_dwh_dim_clients_hist t
    left join demipt2.gold_stg_dim_clients_del s
    on t.client_id = s.client_id and t.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' ) and deleted_flg = 'N'
    where s.client_id is null
)
and effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
and deleted_flg = 'N';

-- 6. Обновляем метаданные.
update demipt2.gold_meta_bank
set last_update_dt = ( select max(update_dt) from demipt2.gold_stg_dim_clients )
where
    1=1
    and table_db = 'bank' and table_name = 'clients'
    and ( select max(update_dt) from demipt2.gold_stg_dim_clients ) is not null;

-- 5. Фиксируем транзакцию.
commit;