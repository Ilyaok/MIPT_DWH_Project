-- создание таблицы метаданных
create table demipt2.gold_meta_bank (
    table_db varchar2(30),
    table_name varchar2(30),
    last_update_dt date
);

-- вставка данных для первичной загрузки с источников в таблицу метаданных
insert into demipt2.gold_meta_bank(table_db, table_name, last_update_dt)
values ( 'bank', 'accounts',  to_date( '1900-01-01', 'yyyy-mm-dd') );

insert into demipt2.gold_meta_bank(table_db, table_name, last_update_dt)
values ( 'bank', 'cards',  to_date( '1900-01-01', 'yyyy-mm-dd') );

insert into demipt2.gold_meta_bank(table_db, table_name, last_update_dt)
values ( 'bank', 'clients',  to_date( '1900-01-01', 'yyyy-mm-dd') );

insert into demipt2.gold_meta_bank(table_db, table_name, last_update_dt)
values ( 'bank', 'terminals',  to_date( '1900-01-01', 'yyyy-mm-dd') );