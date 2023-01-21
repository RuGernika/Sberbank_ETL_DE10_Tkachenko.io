CREATE TABLE IF NOT EXISTS de10.tkch_stg_accounts (
	account_num     CHAR(20),
	valid_to    DATE,
	client      VARCHAR(10),
	update_dt   TIMESTAMP(0) 
);


CREATE TABLE IF NOT EXISTS de10.tkch_stg_accounts_del (
	account_num CHAR(20)
);


CREATE TABLE IF NOT EXISTS de10.tkch_dwh_dim_accounts_hist (
       account_num           CHAR(20),
       valid_to          DATE,
       client            VARCHAR(10),
       effective_from    TIMESTAMP(0),
       effective_to      TIMESTAMP(0),
       deleted_flg       CHAR(1)
);



CREATE TABLE IF NOT EXISTS de10.tkch_stg_passport_blacklist (
       passport_num   VARCHAR(11), 
       entry_dt       DATE
);



CREATE TABLE  IF NOT EXISTS de10.tkch_stg_cards (
	card_num       CHAR(20),
	account_num        CHAR(20),
	update_dt      TIMESTAMP(0) 
);


CREATE TABLE IF NOT EXISTS de10.tkch_stg_cards_del (
       card_num CHAR(20)
   
); 

CREATE TABLE IF NOT EXISTS de10.tkch_dwh_dim_cards_hist (
       card_num          CHAR(20),
       account_num           CHAR(20),
       effective_from    TIMESTAMP(0),
       effective_to      TIMESTAMP(0),
       deleted_flg       CHAR(1)
);


 

CREATE TABLE  IF NOT EXISTS de10.tkch_stg_clients (
	client_id           VARCHAR(10),
	last_name           VARCHAR(20),
	first_name          VARCHAR(20),
	patronymic          VARCHAR(20),  -- 
	date_of_birth       DATE,
	passport_num        VARCHAR(15),
	passport_valid_to   DATE,
	phone               CHAR(16),
	update_dt           TIMESTAMP(0) 
);


CREATE TABLE  IF NOT EXISTS de10.tkch_stg_clients_del (
       client_id VARCHAR(10)
);


CREATE TABLE  IF NOT EXISTS  de10.tkch_dwh_dim_clients_hist (
	client_id            VARCHAR(10),
	last_name            VARCHAR(20),
	first_name           VARCHAR(20),
	patronymic           VARCHAR(20),
	date_of_birth        DATE,
	passport_num         VARCHAR(15),
	passport_valid_to    DATE,
	phone                CHAR(16),
        effective_from       TIMESTAMP(0),
        effective_to         TIMESTAMP(0),
        deleted_flg          CHAR(1)
);



CREATE TABLE  IF NOT EXISTS de10.tkch_stg_terminals (
       terminal_id          VARCHAR(7), 
       terminal_type        CHAR(3), 
       terminal_city        VARCHAR(50), 
       terminal_address     VARCHAR(100), 
       update_dt            TIMESTAMP(0) 
);


CREATE TABLE  IF NOT EXISTS de10.tkch_dwh_dim_terminals_hist (
       terminal_id          VARCHAR(7), 
       terminal_type        CHAR(3), 
       terminal_city        VARCHAR(50), 
       terminal_address     VARCHAR(100), 
       effective_from       TIMESTAMP(0),
       effective_to         TIMESTAMP(0),
       deleted_flg          CHAR(1)
);


CREATE TABLE IF NOT EXISTS de10.tkch_stg_transactions (
      trans_id     VARCHAR(20), 
      trans_date   TIMESTAMP(0), 
      card_num     VARCHAR(19), 
      amt          DECIMAL(7,2), 
      oper_type    VARCHAR(15), 
      oper_result  VARCHAR(10), 
      terminal     VARCHAR(7)
); 


CREATE TABLE IF NOT EXISTS de10.tkch_dwh_fact_transactions
(
      trans_id     VARCHAR(20), 
      trans_date   TIMESTAMP(0), 
      card_num     VARCHAR(19), 
      amt          DECIMAL(7,2), 
      oper_type    VARCHAR(15), 
      oper_result  VARCHAR(10), 
      terminal     VARCHAR(7)
); 


CREATE TABLE IF NOT EXISTS  de10.tkch_dwh_fact_passport_blacklist (
       passport_num      VARCHAR(11), 
       entry_dt          DATE
);



CREATE TABLE  IF NOT EXISTS de10.tkch_rep_fraud (
    event_dt     TIMESTAMP(0), 
    passport     VARCHAR(11), 
    fio          VARCHAR(128), 
    phone        VARCHAR(20), 
    event_type   VARCHAR(100), 
    report_dt    TIMESTAMP(0)
);


CREATE TABLE IF NOT EXISTS de10.tkch_meta(
    schema_name varchar(30),
    table_name varchar(30),
    max_update_dt timestamp(0)
);


INSERT INTO de10.tkch_meta( schema_name, table_name, max_update_dt) VALUES( 'info','accounts', to_timestamp('1900-01-01','YYYY-MM-DD'));
INSERT INTO de10.tkch_meta( schema_name, table_name, max_update_dt) VALUES( 'info','clients', to_timestamp('1900-01-01','YYYY-MM-DD'));
INSERT INTO de10.tkch_meta( schema_name, table_name, max_update_dt) VALUES( 'info','cards', to_timestamp('1900-01-01','YYYY-MM-DD'));
INSERT INTO de10.tkch_meta( schema_name, table_name, max_update_dt) VALUES( 'file_fk','trnsactions', to_timestamp('1900-01-01','YYYY-MM-DD'));
INSERT INTO de10.tkch_meta( schema_name, table_name, max_update_dt) VALUES( 'file_fk','passport_black_list', to_timestamp('1900-01-01','YYYY-MM-DD'));
INSERT INTO de10.tkch_meta( schema_name, table_name, max_update_dt) VALUES( 'file','terminals', to_timestamp('1900-01-01','YYYY-MM-DD'));

COMMIT;
