-- Инкрементальная загрузка данных по аккаунтам

-- 1. очистка стейджингов.
delete from demipt2.gold_stg_dim_accounts;
delete from demipt2.gold_stg_dim_accounts_del;

-- 2. захват данных в стейджинг (кроме удалений).
insert into demipt2.gold_stg_dim_accounts (
    account_num,
    valid_to,
    client,
    create_dt,
    update_dt
)
select
    account,
    valid_to,
    client,
    create_dt,
    case
    when update_dt is NULL then create_dt
    else current_date end as update_dt
from bank.accounts
where 1=0
    or update_dt > (
    select coalesce( last_update_dt, to_date( '1900-01-01', 'yyyy-mm-dd') )
    from demipt2.gold_meta_bank where table_db = 'bank' and table_name = 'accounts' )
    or update_dt is null;

-- 3. захват ключей для вычисления удалений.
insert into demipt2.gold_stg_dim_accounts_del ( account_num )
select account from bank.accounts;

-- 4. выделяем "вставки" и "обновления" и вливаем их в приемник

-- 4.1. вставка новой строки или закрытие текущей версии по scd2
merge into demipt2.gold_dwh_dim_accounts_hist tgt
using (
    select
        s.account_num,
        s.valid_to,
        s.client,
        s.update_dt,
        'n' as  deleted_flg,
        s.update_dt as effective_from,
        to_date( '9999-01-01', 'yyyy-mm-dd') as effective_to
    from demipt2.gold_stg_dim_accounts s
    left join demipt2.gold_dwh_dim_accounts_hist t
    on s.account_num = t.account_num
    where
          t.account_num is null
          or (
            t.account_num is not null
            and (1 = 0
                     or (s.valid_to <> t.valid_to) or (s.valid_to is null and t.valid_to is not null) or (s.valid_to is not null and t.valid_to is null)
                     or (s.client <> t.client) or (s.client is null and t.client is not null) or (s.client is not null and t.client is null)
                )
             )
) stg
on ( tgt.account_num = stg.account_num )
when not matched then insert (
    account_num,
    valid_to,
    client,
    deleted_flg,
    effective_from,
    effective_to
    )
values (
    stg.account_num,
    stg.valid_to,
    stg.client,
    'n',
    stg.effective_from,
    to_date( '9999-01-01', 'yyyy-mm-dd')
    )
when matched then
update set effective_to = current_date - interval '1' second;


-- 4.2. вставка новой версии по scd2 для случая апдейта
insert into demipt2.gold_dwh_dim_accounts_hist (
    account_num,
    valid_to,
    client,
    deleted_flg,
    effective_from,
	effective_to
)
select
    s.account_num,
    s.valid_to,
    s.client,
    'n' as  deleted_flg,
    current_date as effective_from,
    to_date( '9999-01-01', 'yyyy-mm-dd') as effective_to
from demipt2.gold_stg_dim_accounts s
left join demipt2.gold_dwh_dim_accounts_hist t
on s.account_num = t.account_num
where
    t.account_num is null
    or (
    t.account_num is not null
    and (1 = 0
             or (s.valid_to <> t.valid_to) or (s.valid_to is null and t.valid_to is not null) or (s.valid_to is not null and t.valid_to is null)
        )
    and (1 = 0
             or (s.client <> t.client) or (s.client is null and t.client is not null) or (s.client is not null and t.client is null)
        )
     )
    and effective_to <> to_date( '9999-01-01', 'yyyy-mm-dd');


-- 5. проставляем в приемнике флаг для удаленных записей ('y' - для удаленных)

-- 5.1. вставляем актуальную запись по scd2
insert into demipt2.gold_dwh_dim_accounts_hist (
    account_num,
    valid_to,
    client,
	deleted_flg,
	effective_from,
	effective_to
)
select
    account_num,
    valid_to,
    client,
	'y' as deleted_flg,
	current_date as effective_from,
	to_date( '9999-01-01', 'yyyy-mm-dd') as effective_to
from demipt2.gold_dwh_dim_accounts_hist
where account_num in (
    select
        t.account_num
    from demipt2.gold_dwh_dim_accounts_hist t
    left join demipt2.gold_stg_dim_accounts_del s
    on t.account_num = s.account_num
    where s.account_num is null
	)
	and deleted_flg = 'n'
	and effective_to = to_date( '9999-01-01', 'yyyy-mm-dd')

;

-- 5.2. обновляем данные об удаленной записи по scd2
update demipt2.gold_dwh_dim_accounts_hist
set effective_to = current_date - interval '1' second
where account_num in (
    select
        t.account_num
    from demipt2.gold_dwh_dim_accounts_hist t
    left join demipt2.gold_stg_dim_accounts_del s
    on t.account_num = s.account_num
    where s.account_num is null
	)
	and deleted_flg = 'n'
	and effective_to = to_date( '9999-01-01', 'yyyy-mm-dd')
;


-- 6. обновляем метаданные.
update demipt2.gold_meta_bank
set last_update_dt = ( select max(effective_from) from demipt2.gold_dwh_dim_accounts_hist )
where table_db = 'bank' and table_name = 'accounts' and ( select max(effective_from) from demipt2.gold_dwh_dim_accounts_hist ) is not null;


-- 7. фиксируем транзакцию.
commit;
