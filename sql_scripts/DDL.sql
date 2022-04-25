-- Создание требуемых в задании таблиц

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
