-- Создание всех конечных таблиц для загрузки данных

-- таблица-отчет по мошенническим операциям
create table demipt2.gold_rep_fraud (
    event_dt date,
    passport varchar2(20),
    fio varchar2(100),
    phone varchar(20),
    event_type varchar2(100),
    report_dt date
);

-- таблица фактов - транзакции
create table demipt2.gold_dwh_fact_transactions (
    trans_id varchar2(30),
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
create table demipt2.gold_dwh_fact_pssprt_blcklst (
    passport_num varchar2(20),
    entry_dt date
);

-- таблица измерений в SCD2 - терминалы
create table demipt2.gold_dwh_dim_terminals_hist (
    terminal_id varchar2(10),
    terminal_type varchar2(10),
    terminal_city varchar2(50),
    terminal_address varchar2(100),
    deleted_flg char(1),
    effective_from date,
	effective_to date
);

-- таблица измерений в SCD2 - карты
create table demipt2.gold_dwh_dim_cards_hist (
    card_num varchar2(20),
    account_num varchar2(50),
    deleted_flg char(1),
    effective_from date,
	effective_to date
);

-- таблица измерений в SCD2 - аккаунты
create table demipt2.gold_dwh_dim_accounts_hist (
    account_num varchar2(50),
    valid_to date,
    client varchar2(10),
    deleted_flg char(1),
    effective_from date,
	effective_to date
);

-- таблица измерений в SCD2 - клиенты
create table demipt2.gold_dwh_dim_clients_hist (
    client_id varchar2(10),
    last_name varchar2(30),
    first_name varchar2(30),
    patronymic varchar2(30),
    date_of_birth date,
    passport_num varchar2(20),
    passport_valid_to date,
    phone varchar(20),
    deleted_flg char(1),
    effective_from date,
	effective_to date
);
