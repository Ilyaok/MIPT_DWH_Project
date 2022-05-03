-- Технический скрипт для тестового выполнения различных SQL-запросов
commit;

select * from demipt2.gold_rep_fraud;

with full_data as (
    select
        transactions.CARD_NUM,
        cards.CARD_NUM
    from demipt2.gold_dwh_fact_transactions transactions
    left join demipt2.gold_dwh_dim_cards_hist cards on transactions.card_num = cards.card_num
)
select * from full_data
;

--    left join demipt2.gold_dwh_dim_terminals_hist terminals on transactions.terminal = terminals.terminal_id
--     left join demipt2.gold_dwh_dim_cards_hist cards on transactions.card_num = cards.card_num
--     left join demipt2.gold_dwh_dim_accounts_hist accounts on cards.account_num = accounts.account_num
--     left join demipt2.gold_dwh_dim_clients_hist clients on accounts.client = clients.client_id

select * from demipt2.gold_dwh_dim_cards_hist order by CARD_NUM;
select * from demipt2.gold_dwh_fact_transactions order by CARD_NUM;

select *
from demipt2.gold_dwh_dim_cards_hist c
inner join demipt2.gold_dwh_fact_transactions t
on c.CARD_NUM = t.CARD_NUM
order by t.CARD_NUM;

select trim('2121 3653 4477 1121 ') from dual;