-- создание требуемых в задании таблиц

-- стейджинговые таблицы

-- стейдж-таблица - транзакции
create table if not exists demipt2.gold_stg_fact_transactions (
    trans_id varchar2(30) primary key,
    trans_date date,
    card_num varchar2(20),
    oper_type varchar2(20),
    amt decimal,
    oper_result varchar2(20),
    terminal varchar2(20),
    create_dt date,
    update_dt date
);

-- стейдж-таблица - черный список паспортов
create table if not exists demipt2.gold_stg_fact_passport_blacklist (
    passport_num varchar2(20),
    entry_dt date,
    create_dt date,
    update_dt date
);


