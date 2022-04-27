-- Подготовка данных для загрузки данных по аккаунтам

-- Создание стейдж-таблицы
create table demipt2.gold_stg_dim_accounts (
    account_num varchar2(50) primary key,
    valid_to date,
    client varchar2(10),
    create_dt date,
    update_dt date
);

-- Создание таблицы по удалениям
create table demipt2.gold_stg_dim_accounts_del(
    account_num varchar2(50)
);

-- Создание таблицы измерений в SCD2
create table demipt2.gold_dwh_dim_accounts_hist (
    account_num varchar2(50) primary key,
    valid_to date,
    client varchar2(10),
    deleted_flg char(1),
    effective_from date,
	effective_to date
);