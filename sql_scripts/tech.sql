commit;
rollback;

-- update demipt2.gold_meta_bank
-- set last_update_dt = to_date( '1900-01-01', 'yyyy-mm-dd')
-- where table_db = 'demipt2' and table_name = 'gold_dwh_fact_pssprt_blcklst';
--
-- delete from demipt2.gold_stg_pssprt_blcklst;
-- delete from demipt2.gold_stg_pssprt_blcklst_raw;
-- delete from demipt2.gold_dwh_fact_pssprt_blcklst;

select * from demipt2.gold_stg_pssprt_blcklst_raw;
select * from demipt2.gold_stg_pssprt_blcklst;
select * from demipt2.gold_dwh_fact_pssprt_blcklst order by PASSPORT_NUM;

select * from demipt2.gold_meta_bank;

update demipt2.gold_meta_bank
set LAST_UPDATE_DT = to_date( '1900-01-01', 'yyyy-mm-dd')
where table_db = 'demipt2' and table_name = 'gold_dwh_fact_pssprt_blcklst';

-- update demipt2.gold_meta_bank
-- set last_update_dt = to_date( '1900-01-01', 'yyyy-mm-dd')
-- where table_db = 'demipt2'
-- and table_name = 'gold_dwh_dim_terminals_hist';
--
-- delete from demipt2.gold_stg_dim_terminals;
-- delete from demipt2.gold_stg_dim_terminals_raw;
-- delete from demipt2.gold_stg_dim_terminals_del;
-- delete from demipt2.gold_dwh_dim_terminals_hist;

select * from demipt2.gold_stg_dim_terminals_raw order by TERMINAL_ID;
select * from demipt2.gold_stg_dim_terminals order by TERMINAL_ID;
select * from demipt2.gold_stg_dim_terminals_del order by TERMINAL_ID;
select * from demipt2.gold_dwh_dim_terminals_hist order by TERMINAL_ID, EFFECTIVE_FROM;

delete from demipt2.gold_dwh_dim_terminals_hist
where TERMINAL_ID = 'A8966' and TERMINAL_ADDRESS = 'г. Новоуральск, ул. Степана Шутова, д. 3'
;

select * from demipt2.gold_meta_bank;

select * from DBA_SEGMENTS;

select * from demipt2.gold_rep_fraud;

select * from demipt2.gold_stg_dim_accounts;
select * from demipt2.gold_stg_dim_accounts_del;
select * from demipt2.gold_dwh_dim_accounts_hist;

select * from demipt2.gold_stg_dim_cards;
select * from demipt2.gold_stg_dim_cards_del;
select * from demipt2.gold_dwh_dim_cards_hist;

select * from demipt2.gold_stg_dim_clients;
select * from demipt2.gold_stg_dim_clients_del;
select * from demipt2.gold_dwh_dim_clients_hist;

SELECT * FROM BANK.ACCOUNTS a ;
SELECT * FROM BANK.CARDS c ;
SELECT * FROM BANK.CLIENTS c ;

-- drop table demipt2.gold_stg_dim_accounts;
-- drop table demipt2.gold_stg_dim_accounts_del;
-- drop table demipt2.gold_dwh_dim_accounts_hist;
--
-- drop table demipt2.gold_stg_dim_cards;
-- drop table demipt2.gold_stg_dim_cards_del;
-- drop table demipt2.gold_dwh_dim_cards_hist;
--
-- drop table demipt2.gold_stg_dim_clients;
-- drop table demipt2.gold_stg_dim_clients_del;
-- drop table demipt2.gold_dwh_dim_clients_hist;
--
-- drop table demipt2.gold_stg_dim_terminals;
-- drop table demipt2.gold_stg_dim_terminals_del;
-- drop table demipt2.gold_dwh_dim_terminals_hist;

select coalesce( last_update_dt, to_date( '1900-01-01', 'YYYY-MM-DD') )
from demipt2.gold_meta_bank where table_db = 'bank' and table_name = 'terminals';


