commit;
rollback;

select * from demipt2.gold_stg_dim_terminals_source;

update demipt2.gold_meta_bank
set last_update_dt = to_date( '1900-01-01', 'yyyy-mm-dd')
where table_db = 'bank'
and table_name = 'terminals';

-- delete from demipt2.gold_stg_dim_terminals;
-- delete from demipt2.gold_stg_dim_terminals_source;
-- delete from demipt2.gold_stg_dim_terminals_del;
-- delete from demipt2.gold_dwh_dim_terminals_hist;

select * from demipt2.gold_stg_dim_terminals;
select * from demipt2.gold_stg_dim_terminals_source;
select * from demipt2.gold_stg_dim_terminals_del;
select * from demipt2.gold_dwh_dim_terminals_hist;

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


