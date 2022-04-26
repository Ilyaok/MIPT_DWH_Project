-- Инкрементальная загрузка данных по аккаунтам

-- 1. Очистка стейджингов.
delete from demipt2.gold_stg_dim_accounts;
delete from demipt2.gold_stg_dim_accounts_del;

-- 2. Захват данных в стейджинг (кроме удалений).
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
    coalesce( update_dt, current_date ) as update_dt
from bank.accounts
where 1=0
    or update_dt > (
    select coalesce( last_update_dt, to_date( '1900-01-01', 'YYYY-MM-DD') )
    from demipt2.gold_meta_bank where table_db = 'bank' and table_name = 'accounts' )
    or update_dt is null;


-- 3. Захват ключей для вычисления удалений.
insert into demipt2.gold_stg_dim_accounts_del ( account_num )
select account from bank.accounts;

select * from demipt2.gold_stg_dim_accounts_del;
select * from bank.accounts;

-- 4. Выделяем "вставки" и "обновления" и вливаем их в приемник

merge into demipt2.gold_dwh_dim_accounts_hist tgt
using (
    select
        s.account_num,
        s.valid_to,
        s.client
    from demipt2.gold_stg_dim_accounts s
    left join demipt2.gold_dwh_dim_accounts_hist t
    on s.account_num = t.account_num and t.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' ) and deleted_flg = 'N'
    where
          1=1
          and t.account_num is not null
              and (
                1=0
                or ( s.valid_to <> t.valid_to )
                or ( s.valid_to is null and t.valid_to is not null )
                or ( s.valid_to is not null and t.valid_to is null )
                  )
              and (
                1=0
                or ( s.client <> t.client )
                or ( s.client is null and t.client is not null )
                or ( s.client is not null and t.client is null )
                  )
) stg
on ( tgt.account_num = stg.account_num )
when matched then update set effective_to = stg.update_dt - interval '1' second where t.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
;

insert into demipt2.chrn_target ( id, val, effective_from, effective_to, deleted_flg )
select
	id,
	val,
	update_dt as effective_from,
	to_date( '9999-12-31', 'YYYY-MM-DD' ) as effective_to,
	'N' as deleted_flg
from demipt2.chrn_stg;

-- 5. Удаляем из приемника удаленные записи
insert into demipt2.chrn_target ( id, val, effective_from, effective_to, deleted_flg )
select
	id,
	val,
	current_date() as effective_from,
	to_date( '9999-12-31', 'YYYY-MM-DD' ) as effective_to,
	'Y' as deleted_flg
from (
    select
        t.id,
		t.val
    from demipt2.chrn_target t
    left join demipt2.chrn_stg_del s
    on t.id = s.id and t.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' ) and deleted_flg = 'N'
    where s.id is null
);

update demipt2.chrn_target
set effective_to = current_date() - interval '1' second
where id in (
    select
        t.id
    from demipt2.chrn_target t
    left join demipt2.chrn_stg_del s
    on t.id = s.id and t.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' ) and deleted_flg = 'N'
    where s.id is null
)
and t.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
and t.deleted_flg = 'N';

-- 6. Обновляем метаданные.
update demipt2.chrn_meta
set last_update_dt = ( select max(update_dt) from demipt2.chrn_stg )
where table_db = 'DEMIPT2' and table_name = 'SOURCE' and ( select max(update_dt) from demipt2.chrn_stg ) is not null;

-- 5. Фиксируем транзакцию.
commit;

