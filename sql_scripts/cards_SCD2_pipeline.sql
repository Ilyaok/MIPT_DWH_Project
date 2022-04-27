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
    account,
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
select card_num from bank.cards;


-- 4. Выделяем "вставки" и "обновления" и вливаем их в приемник

-- 4.1. вставка новой строки или закрытие текущей версии по scd2
merge into demipt2.gold_dwh_dim_cards_hist tgt
using (
    select
        s.card_num,
        s.account_num,
        s.update_dt,
        'n' as  deleted_flg,
        s.update_dt as effective_from,
        to_date( '9999-01-01', 'yyyy-mm-dd') as effective_to
    from demipt2.gold_stg_dim_cards s
    left join demipt2.gold_dwh_dim_cards_hist t
    on s.card_num = t.card_num
    where
          1=1
          and t.card_num is null
          or (
            t.card_num is not null
            and (1 = 0
                     or (s.account_num <> t.account_num) or (s.account_num is null and t.account_num is not null)
                     or (s.account_num is not null and t.account_num is null)
                )
             )
) stg
on ( tgt.card_num = stg.card_num )
when not matched then insert (
    card_num,
    account_num,
    deleted_flg,
    effective_from,
    effective_to
    )
values (
    stg.card_num,
    stg.account_num,
    'n',
    stg.effective_from,
    to_date( '9999-01-01', 'yyyy-mm-dd')
    )
when matched then
update set effective_to = current_date - interval '1' second;


-- 4.2. вставка новой версии по scd2 для случая апдейта
insert into demipt2.gold_dwh_dim_cards_hist (
    card_num,
    account_num,
    deleted_flg,
    effective_from,
	effective_to
)
select
    s.card_num,
    s.account_num,
    'n' as  deleted_flg,
    current_date as effective_from,
    to_date( '9999-01-01', 'yyyy-mm-dd') as effective_to
from demipt2.gold_stg_dim_cards s
left join demipt2.gold_dwh_dim_cards_hist t
on s.card_num = t.card_num
where
    1=1
    and t.card_num is null
    or (
    t.card_num is not null
        and (1 = 0
                 or (s.account_num <> t.account_num) or (s.account_num is null and t.account_num is not null)
                 or (s.account_num is not null and t.account_num is null)
            )
     )
    and effective_to <> to_date( '9999-01-01', 'yyyy-mm-dd')
;


-- 5. проставляем в приемнике флаг для удаленных записей ('y' - для удаленных)

-- 5.1. вставляем актуальную запись по scd2
insert into demipt2.gold_dwh_dim_cards_hist (
    card_num,
    account_num,
	deleted_flg,
	effective_from,
	effective_to
)
select
    card_num,
    account_num,
	'y' as deleted_flg,
	current_date as effective_from,
	to_date( '9999-01-01', 'yyyy-mm-dd') as effective_to
from demipt2.gold_dwh_dim_cards_hist
where card_num in (
    select
        t.card_num
    from demipt2.gold_dwh_dim_cards_hist t
    left join demipt2.gold_stg_dim_cards_del s
    on t.card_num = s.card_num
    where s.card_num is null
	)
	and deleted_flg = 'n'
	and effective_to = to_date( '9999-01-01', 'yyyy-mm-dd')

;

-- 5.2. обновляем данные об удаленной записи по scd2
update demipt2.gold_dwh_dim_cards_hist
set effective_to = current_date - interval '1' second
where card_num in (
    select
        t.card_num
    from demipt2.gold_dwh_dim_cards_hist t
    left join demipt2.gold_stg_dim_cards_del s
    on t.card_num = s.card_num
    where s.card_num is null
	)
	and deleted_flg = 'n'
	and effective_to = to_date( '9999-01-01', 'yyyy-mm-dd')
;


-- 6. обновляем метаданные.
update demipt2.gold_meta_bank
set last_update_dt = ( select max(effective_from) from demipt2.gold_dwh_dim_cards_hist )
where table_db = 'bank' and table_name = 'cards' and ( select max(effective_from) from demipt2.gold_dwh_dim_cards_hist ) is not null;


-- 7. фиксируем транзакцию.
commit;