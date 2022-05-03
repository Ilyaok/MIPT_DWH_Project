-- Скрипт для отладки
-- Выборка данных из таблиц и прочие запросы для анализа
commit;
rollback;

select * from dba_segments;

select * from bank.accounts a ;
select * from bank.cards c ;
select * from bank.clients c ;

select * from demipt2.gold_rep_fraud
order by EVENT_DT;

select * from demipt2.gold_rep_fraud;
select count(*) from demipt2.gold_rep_fraud;

select * from demipt2.gold_meta_bank;

select * from demipt2.gold_stg_dim_clients;
select * from demipt2.gold_stg_dim_clients_del;
select * from demipt2.gold_dwh_dim_clients_hist;

select count(*) from bank.clients;
select count(*) from demipt2.gold_dwh_dim_clients_hist;


select * from demipt2.gold_stg_dim_cards;
select * from demipt2.gold_stg_dim_cards_del;
select * from demipt2.gold_dwh_dim_cards_hist;

select count(*) from bank.cards;
select count(*) from demipt2.gold_dwh_dim_cards_hist;


select * from demipt2.gold_stg_dim_accounts;
select * from demipt2.gold_stg_dim_accounts_del;
select * from demipt2.gold_dwh_dim_accounts_hist;

select count(*) from bank.accounts;
select count(*) from demipt2.gold_dwh_dim_accounts_hist;


select * from demipt2.gold_stg_transactions_raw;
select * from demipt2.gold_stg_transactions;
select * from demipt2.gold_dwh_fact_transactions;

select count(*) from demipt2.gold_stg_transactions_raw;
select count(*) from demipt2.gold_stg_transactions;
select count(*) from demipt2.gold_dwh_fact_transactions;

select * from demipt2.gold_stg_pssprt_blcklst_raw;
select * from demipt2.gold_stg_pssprt_blcklst;
select * from demipt2.gold_dwh_fact_pssprt_blcklst order by PASSPORT_NUM;


select * from demipt2.gold_stg_dim_terminals_raw order by TERMINAL_ID;
select * from demipt2.gold_stg_dim_terminals order by TERMINAL_ID;
select * from demipt2.gold_stg_dim_terminals_del order by TERMINAL_ID;
select * from demipt2.gold_dwh_dim_terminals_hist order by TERMINAL_ID, EFFECTIVE_FROM;




