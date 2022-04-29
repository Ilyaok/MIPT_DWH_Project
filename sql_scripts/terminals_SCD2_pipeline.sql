-- Инкрементальная загрузка данных по терминалам

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

-- 4.1. вставка новой строки или закрытие текущей версии по scd2
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
        s.update_dt,
        'n' as  deleted_flg,
        s.update_dt as effective_from,
        to_date( '9999-01-01', 'yyyy-mm-dd') as effective_to
    from demipt2.gold_stg_dim_clients s
    left join demipt2.gold_dwh_dim_clients_hist t
    on s.client_id = t.client_id
    where
      1=1
      and t.client_id is  null
      or (
      t.client_id is not null
      and ( 1 = 0
          or (s.last_name <> t.last_name) or (s.last_name is null and t.last_name is not null) or (s.last_name is not null and t.last_name is null)
          )
      and ( 1 = 0
                or (s.first_name <> t.first_name) or (s.first_name is null and t.first_name is not null)
                or (s.first_name is not null and t.first_name is null)
          )
      and ( 1 = 0
                or (s.patronymic <> t.patronymic) or (s.patronymic is null and t.patronymic is not null)
                or (s.patronymic is not null and t.patronymic is null)
          )
      and ( 1 = 0
                or (s.date_of_birth <> t.date_of_birth) or (s.date_of_birth is null and t.date_of_birth is not null)
                or (s.date_of_birth is not null and t.date_of_birth is null)
          )
      and ( 1 = 0
                or (s.passport_num <> t.passport_num) or (s.passport_num is null and t.passport_num is not null)
                or (s.passport_num is not null and t.passport_num is null)
          )
      and ( 1 = 0
                or (s.passport_valid_to <> t.passport_valid_to) or
           (s.passport_valid_to is null and t.passport_valid_to is not null)
                or (s.passport_valid_to is not null and t.passport_valid_to is null)
          )
      and (1 = 0
            or (s.phone <> t.phone) or (s.phone is null and t.phone is not null)
               or (s.phone is not null and t.phone is null)
          )
      )
) stg
on ( tgt.client_id = stg.client_id )
when not matched then insert (
    client_id,
    last_name,
    first_name,
    patronymic,
    date_of_birth,
    passport_num,
    passport_valid_to,
    phone,
    deleted_flg,
    effective_from,
    effective_to
    )
values (
    stg.client_id,
    stg.last_name,
    stg.first_name,
    stg.patronymic,
    stg.date_of_birth,
    stg.passport_num,
    stg.passport_valid_to,
    stg.phone,
    'n',
    stg.effective_from,
    to_date( '9999-01-01', 'yyyy-mm-dd')
    )
when matched then
update set effective_to = current_date - interval '1' second
;

-- 4.2. вставка новой версии по scd2 для случая апдейта
insert into demipt2.gold_dwh_dim_clients_hist (
    client_id,
    last_name,
    first_name,
    patronymic,
    date_of_birth,
    passport_num,
    passport_valid_to,
    phone,
    deleted_flg,
    effective_from,
	effective_to
)
select
    s.client_id,
    s.last_name,
    s.first_name,
    s.patronymic,
    s.date_of_birth,
    s.passport_num,
    s.passport_valid_to,
    s.phone,
    'n' as  deleted_flg,
    current_date as effective_from,
    to_date( '9999-01-01', 'yyyy-mm-dd') as effective_to
from demipt2.gold_stg_dim_clients s
left join demipt2.gold_dwh_dim_clients_hist t
on s.client_id = t.client_id
   where
      1=1
      and t.client_id is  null
      or (
      t.client_id is not null
      and ( 1 = 0
          or (s.last_name <> t.last_name) or (s.last_name is null and t.last_name is not null) or (s.last_name is not null and t.last_name is null)
          )
      and ( 1 = 0
                or (s.first_name <> t.first_name) or (s.first_name is null and t.first_name is not null)
                or (s.first_name is not null and t.first_name is null)
          )
      and ( 1 = 0
                or (s.patronymic <> t.patronymic) or (s.patronymic is null and t.patronymic is not null)
                or (s.patronymic is not null and t.patronymic is null)
          )
      and ( 1 = 0
                or (s.date_of_birth <> t.date_of_birth) or (s.date_of_birth is null and t.date_of_birth is not null)
                or (s.date_of_birth is not null and t.date_of_birth is null)
          )
      and ( 1 = 0
                or (s.passport_num <> t.passport_num) or (s.passport_num is null and t.passport_num is not null)
                or (s.passport_num is not null and t.passport_num is null)
          )
      and ( 1 = 0
                or (s.passport_valid_to <> t.passport_valid_to) or
           (s.passport_valid_to is null and t.passport_valid_to is not null)
                or (s.passport_valid_to is not null and t.passport_valid_to is null)
          )
      and (1 = 0
            or (s.phone <> t.phone) or (s.phone is null and t.phone is not null)
               or (s.phone is not null and t.phone is null)
          )
      )
    and effective_to <> to_date( '9999-01-01', 'yyyy-mm-dd')
;

-- 5. проставляем в приемнике флаг для удаленных записей ('y' - для удаленных)

-- 5.1. вставляем актуальную запись по scd2
insert into demipt2.gold_dwh_dim_clients_hist (
    client_id,
    last_name,
    first_name,
    patronymic,
    date_of_birth,
    passport_num,
    passport_valid_to,
    phone,
	deleted_flg,
	effective_from,
	effective_to
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
	'y' as deleted_flg,
	current_date as effective_from,
	to_date( '9999-01-01', 'yyyy-mm-dd') as effective_to
from demipt2.gold_dwh_dim_clients_hist
where client_id in (
    select
        t.client_id
    from demipt2.gold_dwh_dim_clients_hist t
    left join demipt2.gold_stg_dim_clients_del s
    on t.client_id = s.client_id
    where s.client_id is null
	)
	and deleted_flg = 'n'
	and effective_to = to_date( '9999-01-01', 'yyyy-mm-dd')
;

-- 5.2. обновляем данные об удаленной записи по scd2
update demipt2.gold_dwh_dim_clients_hist
set effective_to = current_date - interval '1' second
where client_id in (
    select
        t.client_id
    from demipt2.gold_dwh_dim_clients_hist t
    left join demipt2.gold_stg_dim_clients_del s
    on t.client_id = s.client_id
    where s.client_id is null
	)
	and deleted_flg = 'n'
	and effective_to = to_date( '9999-01-01', 'yyyy-mm-dd')
;

-- 6. Обновляем метаданные.
update demipt2.gold_meta_bank
set last_update_dt = ( select max(effective_from) from demipt2.gold_dwh_dim_clients_hist )
where table_db = 'bank' and table_name = 'clients' and ( select max(effective_from) from demipt2.gold_dwh_dim_clients_hist ) is not null;

-- 7. Фиксируем транзакцию.
commit;