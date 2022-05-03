-- Подготовка данных для загрузки данных по картам

-- стейдж-таблица - карты
create table demipt2.gold_stg_dim_cards (
    card_num varchar2(30),
    account_num varchar2(50),
    create_dt date,
    update_dt date
);

-- Создание таблицы по удалениям
create table demipt2.gold_stg_dim_cards_del(
    card_num varchar2(30)
);
