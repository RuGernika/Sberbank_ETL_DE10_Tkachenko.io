INSERT INTO de10.tkch_dwh_fact_passport_blacklist(passport_num , entry_dt)
SELECT 
    stg.passport_num,
    stg.entry_dt
FROM de10.tkch_stg_passport_blacklist stg;


DELETE FROM de10.tkch_stg_passport_blacklist;

