select coalesce( last_update_dt, to_date( '1900-01-01', 'yyyy-mm-dd') )
from demipt2.gold_meta_bank where table_db = 'demipt2' and table_name = 'gold_dwh_fact_pssprt_blcklst';

select last_update_dt
from demipt2.gold_meta_bank where table_db = 'demipt2' and table_name = 'gold_dwh_fact_pssprt_blcklst';