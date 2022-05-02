-- Подготовка данных для загрузки данных по черному списку паспортов

-- Таблица для слоя сырых данных
create table demipt2.gold_stg_pssprt_blcklst_raw (
    entry_dt date,
    passport_num varchar2(20)
);

-- стейдж-таблица - черный список паспортов
create table demipt2.gold_stg_pssprt_blcklst (
    passport_num varchar2(20),
    entry_dt date
);
