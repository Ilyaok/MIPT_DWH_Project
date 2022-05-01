merge into demipt2.gold_dwh_dim_terminals_hist tgt
using (
    select
        s.terminal_id,
        s.terminal_type,
        s.terminal_city,
        s.terminal_address,
        s.update_dt,
        'n' as  deleted_flg,
        s.update_dt as effective_from,
        to_date( '9999-01-01', 'yyyy-mm-dd') as effective_to
    from demipt2.gold_stg_dim_terminals s
    left join demipt2.gold_dwh_dim_terminals_hist t
    on s.terminal_id = t.terminal_id
    and t.effective_to = to_date( '9999-01-01', 'yyyy-mm-dd' ) and deleted_flg = 'n'
    where
      t.terminal_id is null
      or (
      t.terminal_id is not null
      and ( 1=0
        or (s.terminal_type <> t.terminal_type) or (s.terminal_type is null and t.terminal_type is not null)
        or (s.terminal_type is not null and t.terminal_type is null)
        or (s.terminal_city <> t.terminal_city) or (s.terminal_city is null and t.terminal_city is not null)
        or (s.terminal_city is not null and t.terminal_city is null)
        or (s.terminal_address <> t.terminal_address) or (s.terminal_address is null and t.terminal_address is not null)
        or (s.terminal_address is not null and t.terminal_address is null)
        )
      )
) stg
on ( tgt.terminal_id = stg.terminal_id )
when not matched then insert (
    terminal_id,
    terminal_type,
    terminal_city,
    terminal_address,
    deleted_flg,
    effective_from,
    effective_to
    )
values (
    stg.terminal_id,
    stg.terminal_type,
    stg.terminal_city,
    stg.terminal_address,
    'n',
    stg.effective_from,
    to_date( '9999-01-01', 'yyyy-mm-dd')
    )
when matched then
update set effective_to = current_date - interval '1' second
where effective_to = to_date( '9999-01-01', 'yyyy-mm-dd')