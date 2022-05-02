-- Подготовка данных для загрузки данных по транзакциям

-- Таблица для слоя сырых данных
create table demipt2.gold_stg_transactions_raw (
    transaction_id varchar2(30),
    transaction_date date,
    amount number,
    card_num varchar2(30),
    oper_type varchar2(20),
    oper_result varchar2(20),
    terminal varchar2(20)
);

-- стейдж-таблица
create table demipt2.gold_stg_transactions (
    trans_id varchar2(30),
    trans_date date,
    card_num varchar2(30),
    oper_type varchar2(20),
    amt number,
    oper_result varchar2(20),
    terminal varchar2(20)
);