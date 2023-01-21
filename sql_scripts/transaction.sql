INSERT INTO de10.tkch_dwh_fact_transactions (
			trans_id,
			trans_date,
			card_num,
			amt,
			oper_type,
			oper_result,
			terminal
			)
SELECT 
	stg.trans_id,
	stg.trans_date,
	stg.card_num,
	stg.amt,
	stg.oper_type,
	stg.oper_result,
	stg.terminal
FROM de10.tkch_stg_transactions stg;


DELETE  FROM  de10.tkch_stg_transactions;
