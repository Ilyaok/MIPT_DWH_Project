--------------------------------------------------------------------
-- Подготовка данных

-- Таблица-источник будет заполнена из Excel-файлов с помощью Pandas
-- DEMIPT2.GOLD_STG_FACT_PASSPORT_BLACKLIST_SOURCE

CREATE TABLE DEMIPT2.GOLD_STG_FACT_PASSPORT_BLACKLIST_DEL (
    passport_num varchar2(20)
);

INSERT INTO DEMIPT2.CHRN_META(table_db, table_name, last_update_dt)
VALUES ( 'DEMIPT2', 'GOLD_STG_FACT_PASSPORT_BLACKLIST_SOURCE',  TO_DATE( '1900-01-01', 'YYYY-MM-DD') );


--------------------------------------------------------------------
-- Инкрементальная загрузка

-- 1. Очистка стейджингов.
DELETE FROM DEMIPT2.GOLD_STG_FACT_PASSPORT_BLACKLIST;
DELETE FROM DEMIPT2.GOLD_STG_FACT_PASSPORT_BLACKLIST_DEL;

-- 2. Захват данных в стейджинг (кроме удалений).
INSERT INTO DEMIPT2.GOLD_STG_FACT_PASSPORT_BLACKLIST (
    passport_num,
    entry_dt
)
SELECT
    passport_num,
    entry_dt
FROM DEMIPT2.GOLD_STG_FACT_PASSPORT_BLACKLIST_SOURCE
WHERE entry_dt > (
    SELECT COALESCE( last_update_dt, to_date( '1900-01-01', 'YYYY-MM-DD') )
    FROM DEMIPT2.GOLD_META_BANK WHERE table_db = 'DEMIPT2' AND table_name = 'GOLD_STG_FACT_PASSPORT_BLACKLIST_SOURCE' );

-- 3. Захват ключей для вычисления удалений.
INSERT INTO DEMIPT2.GOLD_STG_FACT_PASSPORT_BLACKLIST_DEL ( id )
SELECT id FROM DEMIPT2.GOLD_STG_FACT_PASSPORT_BLACKLIST_SOURCE;

-- 4. Выделяем "вставки" и "обновления" и вливаем их в приемник

MERGE INTO DEMIPT2.GOLD_DWH_FACT_PASSPORT_BLACKLIST_HIST tgt
USING (
    SELECT
        s.passport_num,
        s.entry_dt
    FROM DEMIPT2.GOLD_STG_FACT_PASSPORT_BLACKLIST s
    LEFT JOIN DEMIPT2.GOLD_DWH_FACT_PASSPORT_BLACKLIST_HIST t
    ON s.passport_num = t.passport_num AND t.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' ) AND deleted_flg = 'N'
    WHERE
        T.ID IS NOT NULL AND ( 1=0
          OR S.VAL <> T.VAL OR ( S.VAL IS NULL AND T.VAL IS NOT NULL ) OR ( S.VAL IS NOT NULL AND T.VAL IS NULL )
        )
) STG
ON ( TGT.ID = STG.ID )
WHEN MATCHED THEN UPDATE SET EFFECTIVE_TO = STG.UPDATE_DT - INTERVAL '1' SECOND WHERE T.EFFECTIVE_TO = TO_DATE( '9999-12-31', 'YYYY-MM-DD' )
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

