--------------------------------------------------------------------
-- Подготовка данных

create table demipt2.chrn_source(
    id number,
    val varchar2(50),
    update_dt date
)

insert into demipt2.chrn_source( id, val, update_dt ) values ( 1, 'A', current_date );
insert into demipt2.chrn_source( id, val, update_dt ) values ( 2, 'B', current_date );
insert into demipt2.chrn_source( id, val, update_dt ) values ( 5, 'E', current_date );

update demipt2.chrn_source
set val = null, update_dt = current_date
where id = 2;

delete from demipt2.chrn_source where id = 3;

create table demipt2.chrn_target(
    id number,
    val varchar2(50),
    effective_from date,
	effective_to date,
	deleted_flg char(1)
)

create table demipt2.chrn_stg(
    id number,
    val varchar2(50),
    update_dt date
)

create table demipt2.chrn_stg_del(
    id number
)

create table demipt2.chrn_meta(
    table_db varchar2(30),
    table_name varchar2(30),
    last_update_dt date

)

insert into demipt2.chrn_meta(table_db, table_name, last_update_dt) values ( 'DEMIPT2', 'SOURCE',  to_date( '1900-01-01', 'YYYY-MM-DD') );




--------------------------------------------------------------------
-- Инкрементальная загрузка

-- 1. Очистка стейджингов.
delete from demipt2.chrn_stg;
delete from demipt2.chrn_stg_del;

-- 2. Захват данных в стейджинг (кроме удалений).
insert into demipt2.chrn_stg ( id, val, update_dt )
select id, val, update_dt from demipt2.chrn_source
where update_dt > (
    select coalesce( last_update_dt, to_date( '1900-01-01', 'YYYY-MM-DD') )
    from demipt2.chrn_meta where table_db = 'DEMIPT2' and table_name = 'SOURCE' );

-- 3. Захват ключей для вычисления удалений.
insert into demipt2.chrn_stg_del ( id )
select id from demipt2.chrn_source;

-- 4. Выделяем "вставки" и "обновления" и вливаем их в приемник

merge into demipt2.chrn_target tgt
using (
    select
        s.id,
        s.val
    from demipt2.chrn_stg s
    left join demipt2.chrn_target t
    on s.id = t.id and t.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' ) and deleted_flg = 'N'
    where
        t.id is not null and ( 1=0
          or s.val <> t.val or ( s.val is null and t.val is not null ) or ( s.val is not null and t.val is null )
        )
) stg
on ( tgt.id = stg.id )
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

