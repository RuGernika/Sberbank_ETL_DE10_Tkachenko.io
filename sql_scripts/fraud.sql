-- Отчет 1
 INSERT INTO de10.tkch_rep_fraud (
	SELECT
		   t4.trans_date AS event_dt,
	       t1.passport_num AS passport,
	       t1.last_name || ' ' || t1.first_name || ' ' || t1.patronymic AS fio,
	       t1.phone AS phone,
	       '1' AS event_type,
	       date('now') AS report_dt
	FROM de10.tkch_dwh_dim_clients_hist t1
	LEFT JOIN de10.tkch_dwh_dim_accounts_hist t2 ON t1.client_id = t2.client
	AND t2.deleted_flg ='N'
	LEFT JOIN de10.tkch_dwh_dim_cards_hist t3 ON t2.account_num = t3.account_num
	AND t3.deleted_flg ='N'
	LEFT JOIN de10.tkch_dwh_fact_transactions t4 ON t3.card_num = t4.card_num
	LEFT JOIN de10.tkch_dwh_fact_passport_blacklist t5 ON t5.passport_num = t1.passport_num
	WHERE (t5.entry_dt <= t4.trans_date
	       OR t1.passport_valid_to <= t4.trans_date)
	  AND t4.oper_result = 'SUCCESS'
	ORDER BY t4.trans_date);


-- Отчет 2

	INSERT INTO de10.tkch_rep_fraud (
	SELECT
		   t4.trans_date AS event_dt,
	       t1.passport_num AS passport,
	       t1.last_name || ' ' || t1.first_name || ' ' || t1.patronymic AS fio,
	       t1.phone AS phone,
	       '2' AS event_type,
	       date('now') AS report_dt
	FROM de10.tkch_dwh_dim_clients_hist t1
	left JOIN de10.tkch_dwh_dim_accounts_hist t2 ON t1.client_id = t2.client
	and t2.deleted_flg ='N'
	inner JOIN de10.tkch_dwh_dim_cards_hist t3 ON t2.account_num = t3.account_num
	and t3.deleted_flg ='N'
	left JOIN de10.tkch_dwh_fact_transactions t4 ON t3.card_num = t4.card_num
	and t3.deleted_flg = 'N' and t4.trans_date between t3.effective_from and t3.effective_to
	left JOIN de10.tkch_dwh_fact_passport_blacklist t5 ON t5.passport_num = t1.passport_num
	WHERE (t2.valid_to  <= t4.trans_date
	       OR t1.passport_valid_to <= t4.trans_date)
	  AND t4.oper_result = 'SUCCESS'
	ORDER BY t4.trans_date);


-- Отчет 3 
	INSERT INTO de10.tkch_rep_fraud (
	with t5 AS
(
    SELECT t1.trans_id as trans_id
    FROM  de10.tkch_dwh_fact_transactions t1
    INNER JOIN de10.tkch_dwh_fact_transactions t2
        ON t1.card_num = t2.card_num
        AND extract(epoch from t1.trans_date) - extract(epoch from t2.trans_date) BETWEEN 0 and 3600
    INNER JOIN de10.tkch_dwh_dim_terminals_hist term ON t2.terminal = term.terminal_id
        AND term.deleted_flg = 'N' AND t2.trans_date BETWEEN term.effective_from AND term.effective_to
    WHERE cast(t1.trans_date as date) = '{0}'
        AND t1.oper_result = 'SUCCESS' and t2.oper_result = 'SUCCESS'
    GROUP BY t1.trans_id
    HAVING  count(distinct term.terminal_city) > 1
)
SELECT t4.trans_date AS event_dt,
	       t1.passport_num AS passport,
	       t1.last_name || ' ' || t1.first_name || ' ' || t1.patronymic AS fio,
	       t1.phone AS phone,
	       '3' AS event_type,
	       date('now') AS report_dt
	FROM de10.tkch_dwh_dim_clients_hist t1
	left JOIN de10.tkch_dwh_dim_accounts_hist t2 ON t1.client_id = t2.client
	and t2.deleted_flg ='N'
	left JOIN de10.tkch_dwh_dim_cards_hist t3 ON t2.account_num = t3.account_num
	and t3.deleted_flg ='N'
	left JOIN de10.tkch_dwh_fact_transactions t4 ON t3.card_num = t4.card_num
	and t3.deleted_flg = 'N' and t4.trans_date between t3.effective_from and t3.effective_to
	inner join t5 on  t4.trans_id = t5.trans_id) ;


-- Отчет 4
WITH
stg_amt_current_date AS  (
  SELECT t1.trans_date,
  	   t1.trans_id,
          t1.amt,
          t1.oper_result,
          t2.account_num
   FROM de10.tkch_dwh_fact_transactions t1
   JOIN de10.tkch_dwh_dim_cards_hist t2 ON t1.card_num = t2.card_num
   AND '{0}' BETWEEN t2.effective_from AND t2.effective_to
   WHERE t1.trans_date BETWEEN
       (SELECT min(trans_date)- interval '20 minutes'
        FROM de10.tkch_dwh_fact_transactions
        WHERE  date(trans_date) = to_date( '{0}', 'YYYY-MM-DD')) AND
       (SELECT max(trans_date)
        FROM de10.tkch_dwh_fact_transactions
        WHERE date(trans_date) = to_date( '{0}', 'YYYY-MM-DD'))
    ),
stg_amt_f4 AS  (
  SELECT account_num,
          trans_date,
          trans_id,
          oper_result,
          amt,
          LAG(oper_result, 1) OVER w AS prev_r_1,
          LAG(amt, 1) OVER w AS prev_a_1,
          LAG(oper_result, 2) OVER w AS prev_r_2,
          LAG(amt, 2) OVER w AS prev_a_2,
          LAG(oper_result, 3) OVER w AS prev_r_3,
          LAG(amt, 3) OVER w AS prev_a_3,
          LAG(trans_date, 3) OVER w AS prev_date_3
   FROM stg_amt_current_date
   WINDOW w AS (PARTITION BY account_num
                                          ORDER BY trans_date)
    ),
stg_date_fraud AS  (
  SELECT account_num,
          trans_date AS event_dt
   FROM stg_amt_f4
   WHERE oper_result = 'SUCCESS'
     AND prev_r_1 = 'REJECT'
     AND prev_r_2 = 'REJECT'
     AND prev_r_3 = 'REJECT'
     AND amt < prev_a_1
     AND prev_a_1 < prev_a_2
     AND prev_a_2 < prev_a_3
     AND trans_date < (prev_date_3 + interval '20 minutes')
     ),
stg_current_clients AS  (
  SELECT t1.account_num,
          t2.passport_num AS passport,
          (t2.last_name || ' ' || t2.first_name || ' ' || t2.patronymic) AS fio,
          t2.phone,
          4 AS event_type
   FROM de10.tkch_dwh_dim_accounts_hist t1
   INNER JOIN de10.tkch_dwh_dim_clients_hist t2 ON t1.client =t2.client_id
   WHERE to_date('{0}', 'YYYY-MM-DD') BETWEEN t1.effective_from AND t1.effective_to
     AND to_date('{0}', 'YYYY-MM-DD')  BETWEEN t2.effective_from AND t2.effective_to
     ),
fraud_4 AS (
  SELECT t1.event_dt,
        t2.passport,
        t2.fio,
        t2.phone,
        t2.event_type
  FROM stg_date_fraud t1
  LEFT JOIN stg_current_clients t2 ON t1.account_num = t2.account_num
  )
INSERT INTO  de10.tkch_rep_fraud  (
            event_dt,
            passport,
            fio,
            phone,
            event_type,
            report_dt
        )
SELECT     event_dt,
           passport,
           fio,
           phone,
           event_type,
           now()
FROM       fraud_4;

