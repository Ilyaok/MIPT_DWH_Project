-- создание таблицы метаданных
create table demipt2.gold_meta_bank (
    table_db varchar2(50),
    table_name varchar2(50),
    last_update_dt date
);

-- вставка данных для первичной загрузки с источников в таблицу метаданных
insert into demipt2.gold_meta_bank(table_db, table_name, last_update_dt)
values ( 'demipt2', 'gold_dwh_fact_transactions',  to_date( '1900-01-01', 'yyyy-mm-dd') );

insert into demipt2.gold_meta_bank(table_db, table_name, last_update_dt)
values ( 'demipt2', 'gold_dwh_fact_pssprt_blcklst',  to_date( '1900-01-01', 'yyyy-mm-dd') );

insert into demipt2.gold_meta_bank(table_db, table_name, last_update_dt)
values ( 'demipt2', 'gold_dwh_dim_terminals_hist',  to_date( '1900-01-01', 'yyyy-mm-dd') );

insert into demipt2.gold_meta_bank(table_db, table_name, last_update_dt)
values ( 'demipt2', 'gold_dwh_dim_cards_hist',  to_date( '1900-01-01', 'yyyy-mm-dd') );

insert into demipt2.gold_meta_bank(table_db, table_name, last_update_dt)
values ( 'demipt2', 'gold_dwh_dim_accounts_hist',  to_date( '1900-01-01', 'yyyy-mm-dd') );

insert into demipt2.gold_meta_bank(table_db, table_name, last_update_dt)
values ( 'demipt2', 'gold_dwh_dim_clients_hist',  to_date( '1900-01-01', 'yyyy-mm-dd') );







