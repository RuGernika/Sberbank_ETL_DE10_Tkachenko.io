update de10.tkch_stg_clients
 set update_dt = '1900-12-31'
 where update_dt is null;

insert into de10.tkch_dwh_dim_clients_hist( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg )
select 
	stg.client_id,
	stg.last_name,
	stg.first_name,
	stg.patronymic,
	stg.date_of_birth,
	stg.passport_num,
	stg.passport_valid_to,
	stg.phone,
	stg.update_dt, 
	to_date( '9999-12-31', 'YYYY-MM-DD' ),
	'N'
from de10.tkch_stg_clients stg
left join de10.tkch_dwh_dim_clients_hist tgt
on stg.client_id = tgt.client_id
	and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
	and tgt.deleted_flg = 'N'
where tgt.client_id is null;


update de10.tkch_dwh_dim_clients_hist
set 
	effective_to = tmp.update_dt - interval '1 second'
from (
	select 
		stg.client_id, 
		stg.update_dt
	from de10.tkch_stg_clients stg
	inner join de10.tkch_dwh_dim_clients_hist tgt
	on stg.client_id = tgt.client_id
		and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
		and tgt.deleted_flg = 'N'
	where stg.passport_valid_to <> tgt.passport_valid_to or ( stg.passport_valid_to is null and tgt.passport_valid_to is not null ) or ( stg.passport_valid_to is not null and tgt.passport_valid_to is null )
) tmp
where tkch_dwh_dim_clients_hist.client_id = tmp.client_id; 
