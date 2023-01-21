
update de10.tkch_stg_accounts
 set update_dt = '1900-12-31'
 where update_dt is null;

insert into de10.tkch_dwh_dim_accounts_hist( account, valid_to, client,  effective_from, effective_to, deleted_flg )
select 
	stg.account,
	stg.valid_to,
	stg.client,
	stg.update_dt,
	to_date( '9999-12-31', 'YYYY-MM-DD' ),
	'N'
from de10.tkch_stg_accounts stg
left join de10.tkch_dwh_dim_accounts_hist tgt
on stg.account = tgt.account
	and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
	and tgt.deleted_flg = 'N'
where tgt.account is null;

update de10.tkch_dwh_dim_accounts_hist
set 
	effective_to = tmp.update_dt - interval '1 second'
from (
	select 
		stg.account, 
		stg.update_dt
	from de10.tkch_stg_accounts stg
	inner join de10.tkch_dwh_dim_accounts_hist tgt
	on stg.account = tgt.account
		and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
		and tgt.deleted_flg = 'N'
	where stg.valid_to <> tgt.valid_to or ( stg.valid_to is null and tgt.valid_to is not null ) or ( stg.valid_to is not null and tgt.valid_to is null )
) tmp
where tkch_dwh_dim_accounts_hist.account = tmp.account; 

insert into de10.tkch_dwh_dim_accounts_hist( account, valid_to, effective_from, effective_to, deleted_flg )
select 
	stg.account,
	stg.valid_to,
	stg.update_dt,
	to_date( '9999-12-31', 'YYYY-MM-DD' ),
	'N'
from de10.tkch_stg_accounts stg
inner join de10.tkch_dwh_dim_accounts_hist tgt
on stg.account = tgt.account
	and tgt.effective_to = stg.update_dt - interval '1 second'
	and tgt.deleted_flg = 'N'
where stg.valid_to <> tgt.valid_to or ( stg.valid_to is null and tgt.valid_to is not null ) or ( stg.valid_to is not null and tgt.valid_to is null )


