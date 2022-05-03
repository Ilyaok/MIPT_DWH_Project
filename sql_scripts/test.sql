-- Технический скрипт для тестового выполнения различных SQL-запросов
delete from demipt2.gold_rep_fraud;
commit;

select * from demipt2.gold_rep_fraud;
select count(*) from demipt2.gold_rep_fraud;

select FIO
    from GOLD_REP_FRAUD
        group by fio;

-- Формирование отчета о мошеннических операциях - demipt2.gold_rep_fraud

-- 1. Вставка данных об операциях с просроченными или заблокированными паспортами
merge into demipt2.gold_rep_fraud t1
    using (
        select
            transactions.trans_date as event_dt,
            clients.passport_num as passport,
            clients.last_name || ' ' || clients.first_name || ' ' || clients.patronymic as fio,
            clients.phone as phone,
            1 as event_type,
            current_date as report_dt
        from demipt2.gold_dwh_fact_transactions transactions
                 left join demipt2.gold_dwh_dim_terminals_hist terminals on transactions.terminal = terminals.terminal_id
                 left join demipt2.gold_dwh_dim_cards_hist cards on transactions.card_num = cards.card_num
                 left join demipt2.gold_dwh_dim_accounts_hist accounts on cards.account_num = accounts.account_num
                 left join demipt2.gold_dwh_dim_clients_hist clients on accounts.client = clients.client_id
                 left join demipt2.gold_dwh_fact_pssprt_blcklst pssprt_blcklst on clients.passport_num = pssprt_blcklst.passport_num
        where
            1=1
            and (
                1=1
                and pssprt_blcklst.passport_num is not null
                and pssprt_blcklst.entry_dt is not null
                and transactions.trans_date >= pssprt_blcklst.entry_dt
                )
            or  (
                1=1
                and clients.passport_valid_to is not null
                and transactions.trans_date >= clients.passport_valid_to
                )
    ) t2
    on (1 = 1
        and t1.event_dt = t2.event_dt
        and t1.passport = t2.passport
        and t1.fio = t2.fio
        and t1.phone = t2.phone
        and t1.event_type = t2.event_type
        )
    when not matched then
        insert values (
                        t2.event_dt,
                        t2.passport,
                        t2.fio,
                        t2.phone,
                        t2.event_type,
                        t2.report_dt
                      )
;

-- 2. Совершение операции при недействующем договоре
merge into demipt2.gold_rep_fraud t1
            using (
                select
                    transactions.trans_date as event_dt,
                    clients.passport_num as passport,
                    clients.last_name || ' ' || clients.first_name || ' ' || clients.patronymic as fio,
                    clients.phone as phone,
                    1 as event_type,
                    current_date as report_dt
                from demipt2.gold_dwh_fact_transactions transactions
                         left join demipt2.gold_dwh_dim_terminals_hist terminals on transactions.terminal = terminals.terminal_id
                         left join demipt2.gold_dwh_dim_cards_hist cards on transactions.card_num = cards.card_num
                         left join demipt2.gold_dwh_dim_accounts_hist accounts on cards.account_num = accounts.account_num
                         left join demipt2.gold_dwh_dim_clients_hist clients on accounts.client = clients.client_id
                         left join demipt2.gold_dwh_fact_pssprt_blcklst pssprt_blcklst on clients.passport_num = pssprt_blcklst.passport_num
                where
                    1=1
                    and (
                        1=1
                        and accounts.valid_to is not null
                        and transactions.trans_date >= accounts.valid_to
                        )
            ) t2
            on (1 = 1
                and t1.event_dt = t2.event_dt
                and t1.passport = t2.passport
                and t1.fio = t2.fio
                and t1.phone = t2.phone
                and t1.event_type = t2.event_type
                )
            when not matched then
                insert values (
                                t2.event_dt,
                                t2.passport,
                                t2.fio,
                                t2.phone,
                                t2.event_type,
                                t2.report_dt
                              )
;

-- 3. Совершение операций в разных городах в течение одного часа
merge into demipt2.gold_rep_fraud t1
            using (
                select
                    transactions.trans_date as event_dt,
                    clients.passport_num as passport,
                    clients.last_name || ' ' || clients.first_name || ' ' || clients.patronymic as fio,
                    clients.phone as phone,
                    3 as event_type,
                    current_date as report_dt
                from demipt2.gold_dwh_fact_transactions transactions
                         left join demipt2.gold_dwh_dim_terminals_hist terminals on transactions.terminal = terminals.terminal_id
                         left join demipt2.gold_dwh_dim_cards_hist cards on transactions.card_num = cards.card_num
                         left join demipt2.gold_dwh_dim_accounts_hist accounts on cards.account_num = accounts.account_num
                         left join demipt2.gold_dwh_dim_clients_hist clients on accounts.client = clients.client_id
                         left join demipt2.gold_dwh_fact_pssprt_blcklst pssprt_blcklst on clients.passport_num = pssprt_blcklst.passport_num
                where
                    1=1
                    and (
                        1=1
                        and accounts.valid_to is not null
                        and transactions.trans_date >= accounts.valid_to
                        )
            ) t2
            on (1 = 1
                and t1.event_dt = t2.event_dt
                and t1.passport = t2.passport
                and t1.fio = t2.fio
                and t1.phone = t2.phone
                and t1.event_type = t2.event_type
                )
            when not matched then
                insert values (
                                t2.event_dt,
                                t2.passport,
                                t2.fio,
                                t2.phone,
                                t2.event_type,
                                t2.report_dt
                              )
;

with t1 as (
select
    transactions.trans_date,
    clients.passport_num,
    clients.last_name || ' ' || clients.first_name || ' ' || clients.patronymic as fio,
    clients.phone,
    3 as event_type,
    current_date as report_dt,
    accounts.VALID_TO as acc_valid_to
from demipt2.gold_dwh_fact_transactions transactions
         left join demipt2.gold_dwh_dim_terminals_hist terminals on transactions.terminal = terminals.terminal_id
         left join demipt2.gold_dwh_dim_cards_hist cards on transactions.card_num = cards.card_num
         left join demipt2.gold_dwh_dim_accounts_hist accounts on cards.account_num = accounts.account_num
         left join demipt2.gold_dwh_dim_clients_hist clients on accounts.client = clients.client_id
         left join demipt2.gold_dwh_fact_pssprt_blcklst pssprt_blcklst on clients.passport_num = pssprt_blcklst.passport_num
where 1=1
        and terminals.TERMINAL_CITY is not null
        and terminals.TERMINAL_CITY

order by
    transactions.TRANS_DATE
    )
    select * from t1;
;
