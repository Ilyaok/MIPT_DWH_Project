insert into demipt2.gold_stg_transactions_raw (
                    transaction_id,
                    transaction_date,
                    amount,
                    card_num,
                    oper_type,
                    oper_result,
                    terminal
                    )
                values ('43845789347', to_date('2021-03-01 00:10:34', 'yyyy-mm-dd hh:mi:ss'), 1046, '4513 5880 2369 1799', 'PAYMENT', 'SUCCESS', 'P5456');

insert into demipt2.gold_stg_transactions_raw (
                    transaction_id,
                    transaction_date,
                    amount,
                    card_num,
                    oper_type,
                    oper_result,
                    terminal
                    )
                values ('43845789347', to_date('2018-05-15 8:30:55', 'yyyy-mm-dd hh:mi:ss'), '1046,67', '4513 5880 2369 1799', 'PAYMENT', 'SUCCESS', 'P5456');

SELECT TO_DATE('2015/05/15 8:30:25', 'YYYY/MM/DD HH:MI:SS')
FROM dual;

select * from demipt2.gold_stg_transactions_raw;