-- Скрипт для отладки.
-- Очищает все данные в БД и возвращает к исходному состоянию, до старта любых ETL-процессов.

-- Очистка отчета о мошеннических операциях
delete from demipt2.gold_rep_fraud;

-- Очистка DWH и сброс метаданных - Аккаунты
update demipt2.gold_meta_bank
set last_update_dt = to_date( '1900-01-01', 'yyyy-mm-dd')
where table_db = 'demipt2' and table_name = 'gold_dwh_dim_accounts_hist';

delete from demipt2.gold_stg_dim_accounts;
delete from demipt2.gold_stg_dim_accounts_del;
delete from demipt2.gold_dwh_dim_accounts_hist;

-- Очистка DWH и сброс метаданных - Карты
update demipt2.gold_meta_bank
set last_update_dt = to_date( '1900-01-01', 'yyyy-mm-dd')
where table_db = 'demipt2' and table_name = 'gold_dwh_dim_cards_hist';

delete from demipt2.gold_stg_dim_cards;
delete from demipt2.gold_stg_dim_cards_del;
delete from demipt2.gold_dwh_dim_cards_hist;

-- Очистка DWH и сброс метаданных - Клиенты
update demipt2.gold_meta_bank
set last_update_dt = to_date( '1900-01-01', 'yyyy-mm-dd')
where table_db = 'demipt2' and table_name = 'gold_dwh_dim_clients_hist';

delete from demipt2.gold_stg_dim_clients;
delete from demipt2.gold_stg_dim_clients_del;
delete from demipt2.gold_dwh_dim_clients_hist;

-- Очистка DWH и сброс метаданных - Черный список паспортов
update demipt2.gold_meta_bank
set last_update_dt = to_date( '1900-01-01', 'yyyy-mm-dd')
where table_db = 'demipt2' and table_name = 'gold_dwh_fact_pssprt_blcklst';

delete from demipt2.gold_stg_pssprt_blcklst;
delete from demipt2.gold_stg_pssprt_blcklst_raw;
delete from demipt2.gold_dwh_fact_pssprt_blcklst;

-- Очистка DWH и сброс метаданных - Терминалы
update demipt2.gold_meta_bank
set last_update_dt = to_date( '1900-01-01', 'yyyy-mm-dd')
where table_db = 'demipt2'
and table_name = 'gold_dwh_dim_terminals_hist';

delete from demipt2.gold_stg_dim_terminals;
delete from demipt2.gold_stg_dim_terminals_raw;
delete from demipt2.gold_stg_dim_terminals_del;
delete from demipt2.gold_dwh_dim_terminals_hist;

-- Очистка DWH и сброс метаданных - Транзакции
update demipt2.gold_meta_bank
set last_update_dt = to_date( '1900-01-01', 'yyyy-mm-dd')
where table_db = 'demipt2' and table_name = 'gold_dwh_fact_transactions';

delete from demipt2.gold_stg_transactions_raw;
delete from demipt2.gold_stg_transactions;
delete from demipt2.gold_dwh_fact_transactions;

commit;