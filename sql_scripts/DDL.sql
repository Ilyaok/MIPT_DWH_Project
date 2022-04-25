-- Создание требуемых в задании таблиц

-- Таблицы фактов

-- Таблица фактов - транзакции
CREATE TABLE DEMIPT2.GOLD_DWH_FACT_TRANSACTIONS (
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

-- Таблица фактов - черный список паспортов
CREATE TABLE DEMIPT2.GOLD_DWH_FACT_PASSPORT_BLACKLIST (
    passport_num varchar2(20),
    entry_dt date,
    create_dt date,
    update_dt date
);

-- Таблицы измерений

-- Таблица измерений - терминалы
CREATE TABLE DEMIPT2.GOLD_DWH_DIM_TERMINALS (
    terminal_id varchar2(10) primary key,
    terminal_type varchar2(10),
    terminal_city varchar2(20),
    terminal_address varchar2(100),
    create_dt date,
    update_dt date
);

-- Таблица измерений - карты
CREATE TABLE DEMIPT2.GOLD_DWH_DIM_CARDS (
    card_num varchar2(20) primary key,
    account_num varchar2(50),
    terminal_city varchar2(20),
    terminal_address varchar2(100),
    create_dt date,
    update_dt date
);

-- Таблица измерений - аккаунты
CREATE TABLE DEMIPT2.GOLD_DWH_DIM_ACCOUNTS (
    account_num varchar2(50) primary key,
    valid_to date,
    client varchar2(10),
    create_dt date,
    update_dt date
);

-- Таблица измерений - клиенты
CREATE TABLE DEMIPT2.GOLD_DWH_DIM_CLIENTS (
    client_id varchar2(10) primary key,
    last_name varchar2(30),
    first_name varchar2(30),
    patronymic varchar2(30),
    date_of_birth date,
    passport_num varchar2(20),
    passport_valid_to date,
    phone varchar(20),
    create_dt date,
    update_dt date
);