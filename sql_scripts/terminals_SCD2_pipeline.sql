-- Инкрементальная загрузка данных по терминалам

-- 1. Очистка стейджинга с удаленными значениями
delete from demipt2.gold_stg_dim_terminals;
delete from demipt2.gold_stg_dim_terminals_del;

-- 2. захват данных в стейджинг (кроме удалений).
insert into demipt2.gold_stg_dim_terminals (
    terminal_id,
    terminal_type,
    terminal_city,
    terminal_address,
    update_dt
)
select
    terminal_id,
    terminal_type,
    terminal_city,
    terminal_address,
    update_dt
from demipt2.gold_stg_dim_terminals_source
where 1=0
    or update_dt > (
    select coalesce( last_update_dt, to_date( '1900-01-01', 'YYYY-MM-DD') )
    from demipt2.gold_meta_bank where table_db = 'bank' and table_name = 'terminals' )
    or update_dt is null;

-- 3. Захват ключей для вычисления удалений.
insert into demipt2.gold_stg_dim_terminals_del ( terminal_id )
select terminal_id from demipt2.gold_stg_dim_terminals;

-- 4. Выделяем "вставки" и "обновления" и вливаем их в приемник

-- 4.1. вставка новой строки или закрытие текущей версии по scd2
merge into demipt2.gold_dwh_dim_terminals_hist tgt
using (
    select
        s.terminal_id,
        s.terminal_type,
        s.terminal_city,
        s.terminal_address,
        s.update_dt,
        'n' as  deleted_flg,
        s.update_dt as effective_from,
        to_date( '9999-01-01', 'yyyy-mm-dd') as effective_to
    from demipt2.gold_stg_dim_terminals s
    left join demipt2.gold_dwh_dim_terminals_hist t
    on s.terminal_id = t.terminal_id
    where
      1=1
      and t.terminal_id is  null
      or (
      t.terminal_id is not null
      and ( 1 = 0
            or (s.terminal_type <> t.terminal_type) or (s.terminal_type is null and t.terminal_type is not null)
            or (s.terminal_type is not null and t.terminal_type is null)
          )
      and ( 1 = 0
            or (s.terminal_city <> t.terminal_city) or (s.terminal_city is null and t.terminal_city is not null)
            or (s.terminal_city is not null and t.terminal_city is null)
          )
      and ( 1 = 0
            or (s.terminal_address <> t.terminal_address) or (s.terminal_address is null and t.terminal_address is not null)
            or (s.terminal_address is not null and t.terminal_address is null)
          )
      )
) stg
on ( tgt.terminal_id = stg.terminal_id )
when not matched then insert (
    terminal_id,
    terminal_type,
    terminal_city,
    terminal_address,
    deleted_flg,
    effective_from,
    effective_to
    )
values (
    stg.terminal_id,
    stg.terminal_type,
    stg.terminal_city,
    stg.terminal_address,
    'n',
    stg.effective_from,
    to_date( '9999-01-01', 'yyyy-mm-dd')
    )
when matched then
update set effective_to = current_date - interval '1' second
;

-- 4.2. вставка новой версии по scd2 для случая апдейта
insert into demipt2.gold_dwh_dim_terminals_hist (
    terminal_id,
    terminal_type,
    terminal_city,
    terminal_address,
    deleted_flg,
    effective_from,
	effective_to
)
select
    s.terminal_id,
    s.terminal_type,
    s.terminal_city,
    s.terminal_address,
    'n' as  deleted_flg,
    current_date as effective_from,
    to_date( '9999-01-01', 'yyyy-mm-dd') as effective_to
from demipt2.gold_stg_dim_terminals s
left join demipt2.gold_dwh_dim_terminals_hist t
on s.terminal_id = t.terminal_id
    where
      1=1
      and t.terminal_id is  null
      or (
      t.terminal_id is not null
      and ( 1 = 0
            or (s.terminal_type <> t.terminal_type) or (s.terminal_type is null and t.terminal_type is not null)
            or (s.terminal_type is not null and t.terminal_type is null)
          )
      and ( 1 = 0
            or (s.terminal_city <> t.terminal_city) or (s.terminal_city is null and t.terminal_city is not null)
            or (s.terminal_city is not null and t.terminal_city is null)
          )
      and ( 1 = 0
            or (s.terminal_address <> t.terminal_address) or (s.terminal_address is null and t.terminal_address is not null)
            or (s.terminal_address is not null and t.terminal_address is null)
          )
      )
    and effective_to <> to_date( '9999-01-01', 'yyyy-mm-dd')
;

-- 5. проставляем в приемнике флаг для удаленных записей ('y' - для удаленных)

-- 5.1. вставляем актуальную запись по scd2
insert into demipt2.gold_dwh_dim_terminals_hist (
    terminal_id,
    terminal_type,
    terminal_city,
    terminal_address,
	deleted_flg,
	effective_from,
	effective_to
	)
select
    terminal_id,
    terminal_type,
    terminal_city,
    terminal_address,
	'y' as deleted_flg,
	current_date as effective_from,
	to_date( '9999-01-01', 'yyyy-mm-dd') as effective_to
from demipt2.gold_dwh_dim_terminals_hist
where terminal_id in (
    select
        t.terminal_id
    from demipt2.gold_dwh_dim_terminals_hist t
    left join demipt2.gold_stg_dim_terminals_del s
    on t.terminal_id = s.terminal_id
    where s.terminal_id is null
	)
	and deleted_flg = 'n'
	and effective_to = to_date( '9999-01-01', 'yyyy-mm-dd')
;

-- 5.2. обновляем данные об удаленной записи по scd2
update demipt2.gold_dwh_dim_terminals_hist
set effective_to = current_date - interval '1' second
where terminal_id in (
    select
        t.terminal_id
    from demipt2.gold_dwh_dim_terminals_hist t
    left join demipt2.gold_stg_dim_terminals_del s
    on t.terminal_id = s.terminal_id
    where s.terminal_id is null
	)
	and deleted_flg = 'n'
	and effective_to = to_date( '9999-01-01', 'yyyy-mm-dd')
;

-- 6. Обновляем метаданные.
update demipt2.gold_meta_bank
set last_update_dt = ( select max(effective_from) from demipt2.gold_dwh_dim_terminals_hist )
where table_db = 'bank' and table_name = 'terminals' and ( select max(effective_from) from demipt2.gold_dwh_dim_terminals_hist ) is not null
;

-- 7. Фиксируем транзакцию.
commit;