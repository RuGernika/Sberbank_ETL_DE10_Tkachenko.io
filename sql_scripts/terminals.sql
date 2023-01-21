-- Вставка новых
insert into de10.tkch_dwh_dim_terminals_hist(terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg )
select 
t.terminal_id,
    t.terminal_type,
    t.terminal_city,
    t.terminal_address,
    to_timestamp('1900-12-31', 'YYYY-MM-DD'),
    to_timestamp('9999-12-31', 'YYYY-MM-DD'),
    'N'
from de10.tkch_stg_terminals t;

