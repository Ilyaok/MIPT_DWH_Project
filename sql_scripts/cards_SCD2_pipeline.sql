-- Инкрементальная загрузка данных по картам

-- 1. Очистка стейджингов.
delete from demipt2.gold_stg_dim_cards;
delete from demipt2.gold_stg_dim_cards_del;

-- 2. Захват данных в стейджинг (кроме удалений).
insert into demipt2.gold_stg_dim_cards (
    card_num,
    account_num,
    create_dt,
    update_dt
)
select
    card_num,
    account_num,
    create_dt,
    case
    when update_dt is NULL then create_dt
    else current_date end as update_dt
from bank.cards
where 1=0
    or update_dt > (
    select coalesce( last_update_dt, to_date( '1900-01-01', 'YYYY-MM-DD') )
    from demipt2.gold_meta_bank where table_db = 'bank' and table_name = 'cards' )
    or update_dt is null;


-- 3. Захват ключей для вычисления удалений.
insert into demipt2.gold_stg_dim_cards_del ( card_num )
select account from bank.cards;


-- 4. Выделяем "вставки" и "обновления" и вливаем их в приемник

merge into demipt2.gold_dwh_dim_cards_hist tgt
using (
    select
        s.card_num,
        s.account_num,
        s.update_dt
    from demipt2.gold_stg_dim_cards s
    left join demipt2.gold_dwh_dim_cards_hist t
    on s.card_num = t.card_num and t.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' ) and deleted_flg = 'N'
    where
          1=1
          and t.card_num is not null
              and (
                1=0
                or ( s.account_num <> t.account_num )
                or ( s.account_num is null and t.account_num is not null )
                or ( s.account_num is not null and t.account_num is null )
                  )
) stg
on ( tgt.card_num = stg.card_num )
when matched then update set effective_to = stg.update_dt - interval '1' second
where tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' );


insert into demipt2.gold_dwh_dim_cards_hist (
    card_num,
    account_num,
    effective_from,
	effective_to,
	deleted_flg
)
select
    card_num,
    account_num,
	update_dt as effective_from,
	to_date( '9999-12-31', 'YYYY-MM-DD' ) as effective_to,
	'N' as deleted_flg
from demipt2.gold_stg_dim_cards;

-- 5. Удаляем из приемника удаленные записи
insert into demipt2.gold_dwh_dim_cards_hist (
    card_num,
    account_num,
    effective_from,
	effective_to,
	deleted_flg)
select
    card_num,
    account_num,
	current_date as effective_from,
	to_date( '9999-12-31', 'YYYY-MM-DD' ) as effective_to,
	'Y' as deleted_flg
from (
    select
        t.card_num,
        t.account_num
    from demipt2.gold_dwh_dim_cards_hist t
    left join demipt2.gold_stg_dim_cards_del s
    on t.card_num = s.card_num and t.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' ) and deleted_flg = 'N'
    where s.card_num is null);

update demipt2.gold_dwh_dim_cards_hist
set effective_to = current_date - interval '1' second
where card_num in (
    select
        t.card_num
    from demipt2.gold_dwh_dim_cards_hist t
    left join demipt2.gold_stg_dim_cards_del s
    on t.card_num = s.card_num and t.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' ) and deleted_flg = 'N'
    where s.card_num is null
)
and effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
and deleted_flg = 'N';

-- 6. Обновляем метаданные.
update demipt2.gold_meta_bank
set last_update_dt = ( select max(update_dt) from demipt2.gold_stg_dim_cards )
where
    1=1
    and table_db = 'bank' and table_name = 'cards'
    and ( select max(update_dt) from demipt2.gold_stg_dim_cards ) is not null;

-- 5. Фиксируем транзакцию.
commit;