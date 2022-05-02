-- Подготовка данных для загрузки данных по аккаунтам

-- Создание стейдж-таблицы
create table demipt2.gold_stg_dim_accounts (
    account_num varchar2(50),
    valid_to date,
    client varchar2(10),
    create_dt date,
    update_dt date
);

-- Создание таблицы по удалениям
create table demipt2.gold_stg_dim_accounts_del(
    account_num varchar2(50)
);
