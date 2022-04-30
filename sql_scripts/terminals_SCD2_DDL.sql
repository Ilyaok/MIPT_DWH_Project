-- Подготовка данных для загрузки данных по терминалам

-- стейдж-таблица - терминалы
create table demipt2.gold_stg_dim_terminals (
    terminal_id varchar2(10),
    terminal_type varchar2(10),
    terminal_city varchar2(50),
    terminal_address varchar2(100),
    update_dt date
);

-- Создание таблицы по удалениям
create table demipt2.gold_stg_dim_terminals_del(
    terminal_id varchar2(10)
);