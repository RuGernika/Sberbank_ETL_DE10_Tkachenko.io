#!/usr/bin/python


#==========================================================================#
# ETL ANTIFRAUD PROJECT                                                    #
# TKACHENKO DARIA DE10 TKCH                                                #
#==========================================================================#


import os
import sys
import pandas as pd
import datetime
import psycopg2
import fnmatch, shutil
import logging
sys.path.insert(1, '/home/de10/tkch/project/py_scripts')
from init import connect_bd, close_connect, source_load_config, target_load_config



#-------------------------- Функции для очистки стейджинга и заполенения метаданных -------------------------------------------------------#

def table_init(cursor):
# ''' Разбор структуры базы данных '''

	global stg_list
	global meta

	query_stg = f''' SELECT tablename FROM pg_tables WHERE schemaname='de10' and (tablename like 'tkch_stg%') and (tablename not like '%_del') ; '''
	query_meta = f''' SELECT * FROM de10.tkch_meta; '''

	cursor.execute(query_stg)
	stg_list = cursor.fetchall()
	cursor.execute(query_meta)
	meta = cursor.fetchall()



def drop_stg_table(cursor):
# /*  Очистка стейджинга      */
	for i in stg_list:
		query_drop_stg = f'''DELETE FROM de10.{''.join(i)};'''
		cursor.execute(query_drop_stg)



def load_stg_db(cursor_dwh, cursor_stg, data):
# /*  Загрузка в стейджинг данных . Источник база данных */
	global table_name_list
	for i in range(0,len(meta)):
		shema = meta[i][0]
		table_name = meta[i][1]
		if meta[i][0] == 'info':
			query_select_max_data = f'''SELECT MAX(max_update_dt) FROM {shema_stg}.{meta_table} AS meta  WHERE schema_name='{shema}' AND table_name='{table_name}';'''
			cursor_dwh.execute(query_select_max_data)
			data_update = cursor_dwh.fetchone()


			query_table_name = f''' SELECT column_name FROM information_schema.columns  WHERE table_name ='{table_name}'  AND table_schema = '{shema}' AND  column_name<>'create_dt';'''
			cursor_stg.execute(query_table_name)


			table_name_list = cursor_stg.fetchall()


			query_table_name_stg = f''' SELECT column_name FROM information_schema.columns WHERE table_name ='tkch_stg_{table_name}'  AND table_schema = '{shema_stg}';'''
			cursor_dwh.execute(query_table_name_stg)
			table_name_list_stg = cursor_dwh.fetchall()


			name_lst_stg=[]

			for j in table_name_list_stg:
				name_lst_stg.append(j[0])

			name_lst=[]

			for i in table_name_list:
				name_lst.append(i[0])

			query_select_data_from_db =f''' SELECT  {' ,'.join(name_lst)} FROM {shema}.{table_name}  WHERE update_dt is null or update_dt > '%s' ; '''
			cursor_stg.execute(query_select_data_from_db % str(data))
			records = cursor_stg.fetchall()
			names = [ x[0] for x in cursor_stg.description ]
			df = pd.DataFrame( records, columns = names )


			query_select_data_from_db_delete = f''' SELECT  {name_lst[0]} FROM {shema}.{table_name}  ; '''
			cursor_stg.execute(query_select_data_from_db_delete)
			records = cursor_stg.fetchall()
			names = [ x[0] for x in cursor_stg.description ]
			df_del = pd.DataFrame( records, columns = names )

# ---------- поля таблиц соответсвуют стейджингу
			name_lst = name_lst_stg
			s_v=[]
			n=0
			while n < len(table_name_list):
				s_v.append('%s')
				n= n+1
			query_insert_on_db = f'''INSERT INTO {shema_stg}.tkch_stg_{table_name}  ({' ,'.join(name_lst)}) VALUES ({' ,'.join(s_v)}) '''
			df['update_dt'] = df['update_dt'].where(pd.notnull(df['update_dt']), '1900-01-01')
			cursor_dwh.executemany(query_insert_on_db, df.values.tolist() )


			query_insert_on_db = f'''INSERT INTO {shema_stg}.tkch_stg_{table_name}_del  ({name_lst[0]}) VALUES (%s) '''
			cursor_dwh.executemany(query_insert_on_db, df_del.values.tolist() )

#-- Стейджинг удаления

			query_clean_null = f''' UPDATE {shema_stg}.tkch_stg_{table_name} SET update_dt = '1900-12-31' WHERE update_dt is null; '''
			cursor_dwh.execute(query_clean_null)


# -- загрузка в стейджинг


def scd1_to_scd2(cursor, conn, data):
# /*  Крнвертация таблиц из scd1 в scd2  */
	shema_stg = 'de10'
	for i in range(0,len(meta)):
		shema = meta[i][0]
		table_name = meta[i][1]
		if (meta[i][0] == 'info') or  (meta[i][0] == 'file'):
			query_table_name = f''' SELECT column_name FROM information_schema.columns WHERE table_name ='tkch_stg_{table_name}'  AND table_schema = '{shema_stg}' AND  column_name<>'update_dt';'''
			cursor_dwh.execute(query_table_name)
			table_name_list = cursor_dwh.fetchall()
			name_lst=[]
			for i in table_name_list: name_lst.append(i[0])
			n = 0
			q_loop=[]
# ---------- Вставка
			query_insert = f''' INSERT INTO  {shema_stg}.tkch_dwh_dim_{table_name}_hist ( {' , '.join(name_lst)}, effective_from, effective_to, deleted_flg )
					SELECT  stg.{' , stg.'.join(name_lst)} , stg.update_dt, to_date( '9999-12-31', 'YYYY-MM-DD' ), 'N'
								FROM {shema_stg}.tkch_stg_{table_name} stg
								LEFT JOIN  {shema_stg}.tkch_dwh_dim_{table_name}_hist tgt
								ON stg.{name_lst[0]}= tgt.{name_lst[0]}
									AND tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
									AND tgt.deleted_flg = 'N'
								WHERE tgt.{name_lst[0]} is null; '''
			cursor_dwh.execute(query_insert)



			while n < (len(name_lst)-1):
# ---------- Обновление
				query_update_step_on_while = (f'''stg.{name_lst[n]} <> tgt.{name_lst[n]} OR ( stg.{name_lst[n]} is null and tgt.{name_lst[n]} is not null ) OR
								( stg.{name_lst[n]} IS NOT NULL  AND tgt.{name_lst[n]} IS NULL )  ''')
				n = n+1
				q_loop.append(query_update_step_on_while)
			query_update_step_1 = f'''UPDATE {shema_stg}.tkch_dwh_dim_{table_name}_hist
						    SET effective_to = tmp.update_dt - interval '1 second'
						    FROM (
						    SELECT stg.{' , stg.'.join(name_lst)}, stg.update_dt FROM {shema_stg}.tkch_stg_{table_name} stg
						    INNER JOIN {shema_stg}.tkch_dwh_dim_{table_name}_hist tgt ON stg.{name_lst[0]} = tgt.{name_lst[0]}
						    	AND tgt.effective_to  = to_date( '9999-12-31', 'YYYY-MM-DD' )
						    	AND tgt.deleted_flg = 'N'
						    WHERE  {' OR '.join(q_loop)} ) tmp WHERE {shema_stg}.tkch_dwh_dim_{table_name}_hist.{name_lst[0]} = tmp.{name_lst[0]};

						    INSERT INTO  {shema_stg}.tkch_dwh_dim_{table_name}_hist ( {' , '.join(name_lst)}, effective_from, effective_to, deleted_flg )
					SELECT  stg.{' , stg.'.join(name_lst)} , stg.update_dt, to_date( '9999-12-31', 'YYYY-MM-DD' ), 'N'
								FROM {shema_stg}.tkch_stg_{table_name} stg
								LEFT JOIN  {shema_stg}.tkch_dwh_dim_{table_name}_hist tgt
								ON stg.{name_lst[0]}= tgt.{name_lst[0]}
									AND tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
									AND tgt.deleted_flg = 'N'
								WHERE {' OR '.join(q_loop)}
						    ;
						    '''
			cursor_dwh.execute(query_update_step_1)

# ---------- Удаление

			if table_name != 'terminals':
					query_delete = f''' INSERT INTO  {shema_stg}.tkch_dwh_dim_{table_name}_hist ( {' , '.join(name_lst)}, effective_from, effective_to, deleted_flg )
						SELECT  tgt.{' , tgt.'.join(name_lst)}, to_date('{data}' , 'DD-MM-YYYY' ), to_date( '9999-12-31', 'YYYY-MM-DD' ), 'Y'
						FROM  {shema_stg}.tkch_dwh_dim_{table_name}_hist tgt
						WHERE tgt.{name_lst[0]} IN (
								SELECT tgt.{name_lst[0]} FROM  {shema_stg}.tkch_dwh_dim_{table_name}_hist tgt
								LEFT JOIN {shema_stg}.tkch_stg_{table_name}_del stg ON stg.{name_lst[0]} = tgt.{name_lst[0]}
								WHERE stg.{name_lst[0]} IS null
									AND tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
									AND tgt.deleted_flg = 'N'
						)
						AND tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
						AND tgt.deleted_flg = 'N' ;

					UPDATE  {shema_stg}.tkch_dwh_dim_{table_name}_hist
					SET effective_to = to_date( '{data}', 'DD-MM-YYYY' ) -  interval '1 second'
					WHERE  {shema_stg}.tkch_dwh_dim_{table_name}_hist.{name_lst[0]} IN (
						SELECT tgt.{name_lst[0]}
						FROM {shema_stg}.tkch_dwh_dim_{table_name}_hist tgt
						LEFT JOIN {shema_stg}.tkch_stg_{table_name}_del stg ON stg.{name_lst[0]} = tgt.{name_lst[0]}
						WHERE stg.{name_lst[0]} IS NULL
							AND tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
							AND tgt.deleted_flg = 'N'
					)
					AND {shema_stg}.tkch_dwh_dim_{table_name}_hist.effective_to =  to_date( '9999-12-31', 'YYYY-MM-DD' )
					AND {shema_stg}.tkch_dwh_dim_{table_name}_hist.deleted_flg = 'N'; '''
					cursor_dwh.execute(query_delete)
					query_drop_stg_del = f'''DELETE FROM de10.tkch_stg_{table_name}_del;'''
					cursor.execute(query_drop_stg_del)
					conn.commit()


# ---------- Обновление метаданных


def metadata_db_update(cursor,data_update):
# /*  обновление метаданных     */
	global shema_stg
	global meta_table
	shema_stg ='de10'
	meta_table = 'tkch_meta'


	for i in range(0,len(meta)):
		shema = meta[i][0]
		table_name = meta[i][1]
		if meta[i][0] == 'info' :
			query_meta  =  f''' update {shema_stg}.{meta_table} set max_update_dt = coalesce( (select max( update_dt ) from {shema_stg}.tkch_stg_{table_name}),
	(select max_update_dt from {shema_stg}.{meta_table} where schema_name='{shema}' and table_name='{table_name}' ) ) where schema_name='{shema}' and table_name = '{table_name}' ;'''
			cursor_dwh.execute(query_meta)
		elif meta[i][0] ==  'file' :
			query_meta  =  f''' update {shema_stg}.{meta_table} set max_update_dt = coalesce( (select max( update_dt ) from {shema_stg}.tkch_stg_{table_name}),
	(select max_update_dt from {shema_stg}.{meta_table} where schema_name='{shema}' and table_name='{table_name}' ) ) where schema_name='{shema}' and table_name = '{table_name}' ;'''
			cursor_dwh.execute(query_meta)
		elif meta[i][0] == 'file_fk':
			query_meta  =  f''' update {shema_stg}.{meta_table} set max_update_dt =  '{data_update}' where schema_name='{shema}' and table_name = '{table_name}' ;'''
			cursor_dwh.execute(query_meta)


#--------------|SQL-генератор|-----------------------------------------------------------------------------------#

def upsert(table, **kwargs):
# /*       Upsert       */

    keys = ["%s" % k for k in kwargs]
    values = ["'%s'" % v for v in kwargs.values()]
    sql = list()
    sql.append("INSERT INTO %s (" % table)
    sql.append(", ".join(keys))
    sql.append(") VALUES (")
    sql.append(", ".join(values))
    sql.append(") ON DUPLICATE KEY UPDATE ")
    sql.append(", ".join("%s = '%s'" % (k, v) for k, v in kwargs.iteritems()))
    sql.append(";")
    return "".join(sql)

def read(table, **kwargs):
# /*       Select      */
    sql = list()
    sql.append("SELECT * FROM %s " % table)
    if kwargs:
        sql.append("WHERE " + " AND ".join("%s = '%s'" % (k, v) for k, v in kwargs.iteritems()))
    sql.append(";")
    return "".join(sql)



# --------------------------| Функции обработки данных   |-------------------------------------------------------#

def find_data_file(pattern, path):
# /*  Поиск файлов в каталоге для обработки      */
    result=[]
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern) :
                result.append(os.path.join(root, name))
    return result


def parse_path_file(list_data, pattern):
# /*  Парсинг даты файлов  и подготовка к чтению в Exsel   */
    file_name =[]
    shema=[]
    for file in list_data:
        if pattern == 'passport_blacklist_*.xlsx':
            s = os.path.basename(file)
            shema = 'blacklist'
            date_on_file = s[19:-11]+'-'+s[21:-9]+'-'+s[23:-5]
            file_name.append([file, date_on_file])

        if pattern == 'terminals_*.xlsx':
            s = os.path.basename(file)
            shema = 'terminals'
            date_on_file = s[10:-11]+'-'+s[12:-9]+'-'+s[14:-5]
            file_name.append([file, date_on_file])

        if pattern == 'transactions_*.txt':
            shema = ''
            s = os.path.basename(file)
            date_on_file = s[15:-8]+'-'+s[15:-8]+'-'+s[17:-4]
            file_name.append([file, date_on_file])
    return file_name, shema, date_on_file



def load_one_dataframe_txt (cursor,list_data):
# /*  Загрузка данных терминалов из txt файла   */

        with open(list_data[0]) as f:
             next(f)
             for line in f:
                inner_list = [elt.strip() for elt in line.split(';')]
                df = inner_list[0] , inner_list[1] , inner_list[2].replace(',','.') , inner_list[3] , inner_list[4] , inner_list[5], inner_list[6]
                cursor.execute( "INSERT INTO de10.tkch_stg_transactions (trans_id, trans_date, amt, card_num,   oper_type , oper_result , terminal) VALUES(%s,%s,%s,%s,%s,%s,%s)", df)



def load_one_dataframe_xlsx(cursor,a):
# /*  Загрузка данных  из xlsx  файлов   */

     for i in range(0,len(a[0])):
        s  = (a[0][i],a[0])
        df = pd.read_excel(s[0][0], sheet_name = a[1], header=0, index_col=None )
        if   a[1] == 'terminals':
            df['update_dt'] = s[0][1]
            cursor.executemany( "INSERT INTO de10.tkch_stg_terminals( terminal_id, terminal_type, terminal_city, terminal_address, update_dt ) VALUES( %s, %s, %s, %s, %s)", df.values.tolist() )   


        elif a[1] == 'blacklist' :
            df = df.set_index(df['date'])
            datetime.datetime.strptime(s[0][1], "%d-%m-%Y").date()
            train = df[datetime.datetime.strptime(s[0][1], "%d-%m-%Y").date():  ]
            cursor.executemany( "INSERT INTO de10.tkch_stg_passport_blacklist(  entry_dt, passport_num ) VALUES( %s, %s )", train.values.tolist() )

def make_backup (list_data):
	# /*  Архивация     */
    for file in list_data:
       i =  file + '.blackup'
       os.renames(file, i)
       source_path = os.environ["root_path"] +'/'+ os.path.basename(i)
       destination_path = os.environ["archive_path"]+'/'+ os.path.basename(i)
       shutil.move(source_path, destination_path)


#############################################################################################
# Инициация ETL - процесса.   Extract                                                       #
#############################################################################################

# Время начала работы  процесса.
etl_start_dt = datetime.datetime.now()
print("Дата и время начала выполнения процесса:")
print(etl_start_dt)
print('-------------------------------------------')

# ----------------------------------------------------------------------------------- Подключение к источнику и приемнику данных
try:
	try:
	    conn_src = connect_bd(source_load_config())
	finally:
		print('Подключение к  приемнику выполненно.')
except :
	print('Ошибка подключения к базе данных')

try:
	try:
	    conn_dwh = connect_bd(target_load_config())
	finally:
		print('Подключение к источнику  выполненно.')
except :
	print('Ошибка подключения к базе данных')

cursor_src = conn_src.cursor()
cursor_dwh = conn_dwh.cursor()

# ------------------------------------------------------------------------------------------------------- Проверка состояния DWH
# ------------------------------------------------------------------------ Дата и время последнего успешного выполнения процесса

cursor_dwh.execute( """ SELECT EXISTS ( SELECT FROM  pg_tables  WHERE  schemaname = 'de10' AND  tablename  = 'tkch_meta'); """ )
db_verification = cursor_dwh.fetchone()[0]

if  db_verification == False :
	print('ETL - процесс выполняется впервые. База данных ненайденна. ')
	with conn_dwh.cursor() as cursor_dwh, open(os.environ["ddl_path"], "r",encoding = 'utf-8') as f:
		cursor_dwh.execute(f.read())
		conn_dwh.commit()
		print('DWH-хранилище созданно')
		print('Первичные метаданные для стейджиноговых таблиц установленны на дату 1900-01-01')

else:
	cursor_dwh.execute( """ SELECT MAX(meta.max_update_dt) FROM de10.tkch_meta  AS meta; """ )
	last_etl_dt = cursor_dwh.fetchone()

cursor_src = conn_src.cursor()
cursor_dwh = conn_dwh.cursor()
table_init(cursor_dwh)
# ------------------------------------------------------------------------ Очистка стейджинга
drop_stg_table(conn_dwh.cursor())
conn_dwh.commit()
print('-------------------------------------------')

# ----------------------------------------------------- Получение и проверка списка файлов данных.

list_data1 = find_data_file('terminals_*.xlsx', os.environ["root_path"])
list_data2 = find_data_file('passport_blacklist_*.xlsx', os.environ["root_path"])
list_data3 = find_data_file('transactions_*.txt', os.environ["root_path"])

print('Файлы для обработки найденны')
print(f''' Терминалы: {','.join(list_data1)} ''')
print(f''' Список заблокированных паспортов: {','.join(list_data2)} ''')
print(f''' Транзакции: {','.join(list_data3)} ''')
print('-------------------------------------------')


# Обработка и подготовка исходных данных.   Transform
#----------------------------------------------------------------------------------------------------#
#/*  Входе анализа данных были выявленны следующие  данные подлежащие трансформации.
#	1. Несоответствие порядка полей хранилища transactions и полей файла transactions.txt;
#	2. Использованние ',' для типа decimal в поле amt файла transactions.txt
#	3. Несоответствие поля client_id и client в таблицах clients  источника и приемника,
#	4. Несоответствие поля  account_num  и account в таблицах accounts
#       5. Избыточность данных passport_blacklist.xlsx */

a = parse_path_file (list_data1, 'terminals_*.xlsx' )
b = parse_path_file (list_data2, 'passport_blacklist_*.xlsx' )

file_data = str(a[2])
print('Файлы обрабатываются на дату:' + str(a[2]))
d_for_report = datetime.datetime.strptime(file_data, "%d-%m-%Y").strftime("%Y-%m-%d")
metadata_db_update(conn_dwh.cursor(), a[2])


cursor_dwh = conn_dwh.cursor()
load_one_dataframe_xlsx(cursor_dwh,a)
load_one_dataframe_xlsx(cursor_dwh,b)
load_one_dataframe_txt (cursor_dwh,list_data3)


with conn_dwh.cursor() as cursor_dwh, open(os.environ["sql_script_path"]+'/password.sql') as f: cursor_dwh.execute(f.read())
with conn_dwh.cursor() as cursor_dwh, open(os.environ["sql_script_path"]+'/transaction.sql') as f: cursor_dwh.execute(f.read())
conn_dwh.commit()

cursor_dwh = conn_dwh.cursor()
cursor_stg = conn_src.cursor()
print('Файлы обработанны и загруженны в стейджинг')
print(datetime.datetime.now())
make_backup (list_data1)
make_backup (list_data2)
make_backup (list_data3)
print('Файлы перемещенны в архив')
print(datetime.datetime.now())


load_stg_db(cursor_dwh, cursor_stg, str(a[2]))
conn_dwh.commit()
print('Данные из базы банк обработанны и загруженны в стейджинг ')
print(datetime.datetime.now())
scd1_to_scd2(cursor_dwh,conn_dwh, str(a[2]))
conn_dwh.commit;
print('Данные загруженны в DWH ')
print(datetime.datetime.now())

#################################################################################################################################
# ОТЧЕТЫ

with conn_dwh.cursor() as cursor_dwh, open(os.environ["sql_script_path"]+'/fraud.sql') as f: cursor_dwh.execute(f.read().format(d_for_report))


print('Отчеты построенны')
print(datetime.datetime.now())
conn_dwh.commit;

close_connect(conn_src)
close_connect(conn_dwh)
