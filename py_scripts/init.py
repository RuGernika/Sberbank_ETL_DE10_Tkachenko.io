#!/usr/bin/python

import os
import yaml
import json
import psycopg2


# Загрузка  переменных окружения

config_file = '/home/de10/tkch/project/settings.json'

with open( config_file , "r") as read_config:
    config = json.load(read_config)

os.environ["root_path"] = config['root_path']
os.environ["data_path"] = config['data_path']
os.environ["python_script_path"] = config['python_script_path']
os.environ["sql_script_path"] = config['sql_script_path']
os.environ["archive_path"] = config['archive_path']
os.environ["ddl_path"] = config['ddl_path']
os.environ["flag_first_download"] = '0'


# Загрузка конфигурационных файлов

def source_load_config():
    with open(format(os.environ["root_path"]) + '/db_source.yaml') as file:
        source = yaml.full_load(file)
    return source

def target_load_config():
    with open(format(os.environ["root_path"]) + '/db_target.yaml') as file2:
        target = yaml.full_load(file2)
    return target

# Создание \ завершение подключения к базам данных

def connect_bd(configuration_list):
    conn = psycopg2.connect(
                        database = configuration_list['dbopt']['database'],
                        host =     configuration_list['dbopt']['host'],
                        user =     configuration_list['dbopt']['user'],
                        password = configuration_list['dbopt']['password'],
                        port =     configuration_list['dbopt']['port']
                            )
    conn.autocommit = False
    return conn

def close_connect(conn):
    conn.commit()
    conn.close()
