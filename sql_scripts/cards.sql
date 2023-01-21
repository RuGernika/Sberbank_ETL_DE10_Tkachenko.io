update de10.tkch_stg_cards
set update_dt = '1900-12-31'
where update_dt is null;
 
 
insert into de10.tkch_dwh_dim_cards_hist( card_num, account, effective_from, effective_to, deleted_flg )
select 
	stg.card_num,
	stg.account,
	stg.update_dt, 
	to_date( '9999-12-31', 'YYYY-MM-DD' ),
	'N'
from de10.tkch_stg_cards stg
left join de10.tkch_dwh_dim_cards_hist tgt
on stg.card_num = tgt.card_num
	and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
	and tgt.deleted_flg = 'N'
where tgt.card_num is null;


update de10.tkch_dwh_dim_cards_hist
set 
	effective_to = tmp.update_dt - interval '1 second'
from (
	select 
		stg.card_num, 
		stg.update_dt
	from de10.tkch_stg_cards stg
	inner join de10.tkch_dwh_dim_cards_hist tgt
	on stg.card_num = tgt.card_num
		and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
		and tgt.deleted_flg = 'N'
	where stg.account <> tgt.account or ( stg.account is null and tgt.account is not null ) or ( stg.account is not null and tgt.account is null )
) tmp
where tkch_dwh_dim_cards_hist.card_num = tmp.card_num; 

insert into de10.tkch_dwh_dim_cards_hist( card_num, account, effective_from, effective_to, deleted_flg )
select 
	stg.card_num,
	stg.account,
	stg.update_dt,
	to_date( '9999-12-31', 'YYYY-MM-DD' ),
	'N'
from de10.tkch_stg_cards stg
inner join de10.tkch_dwh_dim_cards_hist tgt
on stg.card_num = tgt.card_num
	and tgt.effective_to = stg.update_dt - interval '1 second'
	and tgt.deleted_flg = 'N'
where stg.account <> tgt.account or ( stg.account is null and tgt.account is not null ) or ( stg.account is not null and tgt.account is null )

