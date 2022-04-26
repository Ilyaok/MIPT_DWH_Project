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

-- стейдж-таблица - терминалы
create table if not exists demipt2.gold_stg_dim_terminals (
    terminal_id varchar2(10) primary key,
    terminal_type varchar2(10),
    terminal_city varchar2(20),
    terminal_address varchar2(100),
    create_dt date,
    update_dt date
);

-- стейдж-таблица - аккаунты
create table if not exists demipt2.gold_stg_dim_accounts (
    account_num varchar2(50) primary key,
    valid_to date,
    client varchar2(10),
    create_dt date,
    update_dt date
);

-- таблицы фактов

-- таблица фактов - транзакции
create table if not exists demipt2.gold_dwh_fact_transactions (
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

-- таблица фактов - черный список паспортов
create table if not exists demipt2.gold_dwh_fact_passport_blacklist_hist (
    passport_num varchar2(20),
    entry_dt date,
    effective_from date,
	effective_to date,
	deleted_flg char(1)
);

-- таблицы измерений

-- таблица измерений - терминалы
create table if not exists demipt2.gold_dwh_dim_terminals_hist (
    terminal_id varchar2(10) primary key,
    terminal_type varchar2(10),
    terminal_city varchar2(20),
    terminal_address varchar2(100),
    create_dt date,
    update_dt date
);

-- таблица-отчет по мошенническим операциям
create table if not exists demipt2.gold_rep_fraud (
    event_dt date,
    passport varchar2(20),
    fio varchar2(100),
    phone varchar(20),
    event_type varchar2(100),
    report_dt date
);