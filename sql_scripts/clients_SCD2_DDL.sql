-- Подготовка данных для загрузки данных по клиентам

-- стейдж-таблица - клиенты
create table demipt2.gold_stg_dim_clients (
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

-- Создание таблицы по удалениям
create table demipt2.gold_stg_dim_clients_del(
    client_id varchar2(50)
);
