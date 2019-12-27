# -*- coding: utf-8 -*-
# -*- Encoding: UTF-8-*-
import glob
from datetime import datetime
import os
import pandas as pd
import pymysql
import shutil


# functions
def insert_head_data(_name, _time):
    csv_data_old = pd.read_csv(_name, delimiter=',', sep='\t', engine='python', encoding='euc - kr', header=None)
    csv_data_old.drop(csv_data_old.filter(regex="Unname"), axis=1, inplace=True)
    csv_data_old.drop(csv_data_old.filter(regex="nan"), axis=1, inplace=True)
    csv_data_old.drop(csv_data_old.filter(regex="\t"), axis=1, inplace=True)
    csv_data_old.dropna(inplace=True)
    cols = csv_data_old.columns.tolist()
    csv_data = csv_data_old[cols]
    header_data_array = []

    header_data_array.append(_time)  # lgmv_date
    header_data_array.append(str(_time[0:10]).replace('-', '.'))  # conn_file_date
    header_data_array.append(str(_time[11:13]) + ':' + str(_time[13:15]) + ':' + str(_time[15:17]))  # conn_file_time
    header_data_array.append(csv_data[1][csv_data[0].str.contains('lgmv_model_name')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('lgmv_serial_number')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('lgmv_model_filter1')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('lgmv_model_filter2')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('lgmv_r')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('lgmv_main_ver')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('lgmv_eep_ver')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('calorimeter_cap')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('calorimeter_power')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('calorimeter_eer')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('calorimeter_power_vol')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('calorimeter_id_wb')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('calorimeter_id_db')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('calorimeter_od_db')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('calorimeter_od_wb')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('idu_chassis')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('odu_chassis')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('plm_step1')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('plm_step2')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('test_step1')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('test_step2')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('conn_tester')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('conn_testroom_number')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('conn_pipe_type')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('conn_test_result')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('conn_memo')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('conn_operation_rate')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('lgmv_odu_count')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('lgmv_idu_count')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('lgmv_hru_count')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('calorimeter_count')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('lgmv_sidu_dxc_count')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('lgmv_sidu_awhp_count')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('lgmv_sidu_showcase_count')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('lgmv_sidu_fau_count')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('lgmv_sidu_whu_count')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('lgmv_sidu_cascade_count')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('lgmv_unit_temp')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('lgmv_unit_press')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('lgmv_unit_flux')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('calorimeter_unit_cap')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('calorimeter_unit_EER')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('summary_graph_id')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('conn_ida_db_gap')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('conn_ida_wb_gap')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('conn_idb_db_gap')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('conn_idb_wb_gap')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('conn_od_db_gap')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('conn_od_wb_gap')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('eer_item_num')].values[0])
    header_data_array.append(csv_data[1][csv_data[0].str.contains('eer_result_params')].values[0])
    conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                           db='lg_work', charset='utf8')
    curs = conn.cursor()
    sql = """insert into header(
    lgmv_date,
conn_file_date,
conn_file_time,
lgmv_model_name,
lgmv_serial_number,
lgmv_model_filter1,
lgmv_model_filter2,
lgmv_r,
lgmv_main_ver,
lgmv_eep_ver,
calorimeter_cap,
calorimeter_power,
calorimeter_eer,
calorimeter_power_vol,
calorimeter_id_wb,
calorimeter_id_db,
calorimeter_od_db,
calorimeter_od_wb,
idu_chassis,
odu_chassis,
plm_step1,
plm_step2,
test_step1,
test_step2,
conn_tester,
conn_testroom_number,
conn_pipe_type,
conn_test_result,
conn_memo,
conn_operation_rate,
lgmv_odu_count,
lgmv_idu_count,
lgmv_hru_count,
calorimeter_count,
lgmv_sidu_dxc_count,
lgmv_sidu_awhp_count,
lgmv_sidu_showcase_count,
lgmv_sidu_fau_count,
lgmv_sidu_whu_count,
lgmv_sidu_cascade_count,
lgmv_unit_temp,
lgmv_unit_press,
lgmv_unit_flux,
calorimeter_unit_cap,
calorimeter_unit_EER,
summary_graph_id,
conn_ida_db_gap,
conn_ida_wb_gap,
conn_idb_db_gap,
conn_idb_wb_gap,
conn_od_db_gap,
conn_od_wb_gap,
eer_item_num,
eer_result_params
    ) values (
%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
%s,%s,%s,%s)"""
    curs.execute(sql, header_data_array)
    conn.commit()
    conn.close()
    print("the end")


def insert_item_data(_name, _time, header_uid):
    csv_data_old = pd.read_csv(_name, delimiter=',', sep='\t', engine='python', encoding='euc - kr')
    csv_data_old["header_uid"] = header_uid
    csv_data_old.drop(csv_data_old.filter(regex="Unname"), axis=1, inplace=True)
    csv_data_old.drop(csv_data_old.filter(regex="nan"), axis=1, inplace=True)
    csv_data_old.drop(csv_data_old.filter(regex="NaN"), axis=1, inplace=True)
    csv_data_old.drop(csv_data_old.filter(regex="\t"), axis=1, inplace=True)
    csv_data_old.dropna(axis=0, inplace=True)
    csv_data_old.dropna(axis=1, inplace=True)
    csv_data_old.reset_index()
    cols = csv_data_old.columns.tolist()
    cols = cols[-1:] + cols[:-1]

    csv_data = csv_data_old[cols]
    # num_of_item = len(csv_data.index) - 1  # item 갯수
    data_list = csv_data.values.tolist()
    conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                           db='lg_work', charset='utf8')
    curs = conn.cursor()
    sql = """insert into Item(
        header_uid,item,name,unit,section,section_count
        ) values(
        %s,%s,%s,%s,%s,%s
        )
        """
    curs.executemany(sql, data_list)
    conn.commit()
    conn.close()
    print("the end")


def insert_raw_data(_name, _time, header_uid):
    csv_data_old = pd.read_csv(_name, delimiter=',', sep='\t', engine='python', encoding='euc - kr')
    csv_data_old["header_uid"] = header_uid
    csv_data_old.drop(csv_data_old.filter(regex="Unname"), axis=1, inplace=True)
    csv_data_old.drop(csv_data_old.filter(regex="nan"), axis=1, inplace=True)
    csv_data_old.drop(csv_data_old.filter(regex="NaN"), axis=1, inplace=True)
    csv_data_old.drop(csv_data_old.filter(regex="\t"), axis=1, inplace=True)
    csv_data_old.dropna(how="all", axis=0, inplace=True)
    csv_data_old.dropna(how="all", axis=1, inplace=True)
    csv_data_old.fillna("0", inplace=True)
    csv_data_old.reset_index()
    cols = csv_data_old.columns.tolist()
    max_item_count = str(cols[-2])[5:]  # item 최대 갯수
    cols = cols[-1:] + cols[:-1]  # header_uid 를 가장 앞으로 빼야 DB에 넣을때 순서대로 들어갈 수 있음.
    # item 이 500개 이하일 경우
    if int(max_item_count) <= 500:
        sql_cols = ','.join(map(str, cols))  # sql 에 col 들을 넣기 위한 string

        sql_s = ["%s"] * len(cols)  # sql에 %s 들을 넣기 위한 string
        sql_s = ','.join(map(str, sql_s))
        csv_data = csv_data_old[cols]
        data_list = csv_data.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()

        sql = "insert into raw_0000_0500("
        sql += sql_cols
        sql += ") values ("
        sql += sql_s
        sql += ")"

        curs.executemany(sql, data_list)
        conn.commit()
        conn.close()
        print("0000~0500 the end")

    # item 이 1000개 이하
    elif int(max_item_count) <= 1000:
        index_500 = cols.index("item_500")  # item 1000이 있는 index
        cols_0000_0500 = cols[:index_500 + 1]  # index_1000에 1을 더해야 접근이 가능
        cols_0501_1000 = cols[index_500 + 1:]  # index_1000에 1을 더해야 접근이 가능
        # cols 에 header_uid , date, time 을 추가해야 함.
        cols_0501_1000.append('header_uid')
        cols_0501_1000.append('Date')
        cols_0501_1000.append('Time')
        # 0000~0500
        sql_0000_0500_cols = ','.join(map(str, cols_0000_0500))  # sql 에 col 들을 넣기 위한 string
        sql_0000_0500_s = ["%s"] * len(cols_0000_0500)  # sql에 %s 들을 넣기 위한 string
        sql_0000_0500_s = ','.join(map(str, sql_0000_0500_s))
        csv_data_0000_0500 = csv_data_old[cols_0000_0500]
        data_list_0000_0500 = csv_data_0000_0500.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()

        sql = "insert into raw_0000_0500("
        sql += sql_0000_0500_cols
        sql += ") values ("
        sql += sql_0000_0500_s
        sql += ")"
        curs.executemany(sql, data_list_0000_0500)
        conn.commit()
        conn.close()
        print("0000~0500 the end")

        # 0501~1000
        sql_0501_1000_cols = ','.join(map(str, cols_0501_1000))  # sql 에 col 들을 넣기 위한 string
        sql_0501_1000_s = ["%s"] * len(cols_0501_1000)  # sql에 %s 들을 넣기 위한 string
        sql_0501_1000_s = ','.join(map(str, sql_0501_1000_s))
        csv_data_0501_1000 = csv_data_old[cols_0501_1000]
        data_list_0501_1000 = csv_data_0501_1000.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_0501_1000("
        sql += sql_0501_1000_cols
        sql += ") values ("
        sql += sql_0501_1000_s
        sql += ")"
        curs.executemany(sql, data_list_0501_1000)
        conn.commit()
        conn.close()
        print("0501~1000 the end")

    # item 이 1500개 이하일 경우
    elif int(max_item_count) <= 1500:
        index_500 = cols.index("item_500")  # item 500이 있는 index
        index_1000 = cols.index("item_1000")  # item 1000이 있는 index

        cols_0000_0500 = cols[:index_500 + 1]  # index_500에 1을 더해야 접근이 가능
        cols_0501_1000 = cols[index_500 + 1:index_1000 + 1]  # index_500에 1을 더해야 접근이 가능
        cols_1001_1500 = cols[index_1000 + 1:]  # index_1000에 1을 더해야 접근이 가능
        # cols 에 header_uid , date, time 을 추가해야 함.
        cols_0501_1000.append('header_uid')
        cols_0501_1000.append('Date')
        cols_0501_1000.append('Time')

        cols_1001_1500.append('header_uid')
        cols_1001_1500.append('Date')
        cols_1001_1500.append('Time')

        # cols_0000_0500
        sql_0000_0500_cols = ','.join(map(str, cols_0000_0500))  # sql 에 col 들을 넣기 위한 string
        sql_0000_0500_s = ["%s"] * len(cols_0000_0500)  # sql에 %s 들을 넣기 위한 string
        sql_0000_0500_s = ','.join(map(str, sql_0000_0500_s))
        csv_data_0000_0500 = csv_data_old[cols_0000_0500]
        data_list_0000_0500 = csv_data_0000_0500.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_0000_0500("
        sql += sql_0000_0500_cols
        sql += ") values ("
        sql += sql_0000_0500_s
        sql += ")"
        curs.executemany(sql, data_list_0000_0500)
        conn.commit()
        conn.close()
        print("0000~0500 the end")

        # 0501~1000
        sql_0501_1000_cols = ','.join(map(str, cols_0501_1000))  # sql 에 col 들을 넣기 위한 string
        sql_0501_1000_s = ["%s"] * len(cols_0501_1000)  # sql에 %s 들을 넣기 위한 string
        sql_0501_1000_s = ','.join(map(str, sql_0501_1000_s))
        csv_data_0501_1000 = csv_data_old[cols_0501_1000]
        data_list_0501_1000 = csv_data_0501_1000.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_0501_1000("
        sql += sql_0501_1000_cols
        sql += ") values ("
        sql += sql_0501_1000_s
        sql += ")"
        curs.executemany(sql, data_list_0501_1000)
        conn.commit()
        conn.close()
        print("0501~1000 the end")

        # 1001~1500
        sql_1001_1500_cols = ','.join(map(str, cols_1001_1500))  # sql 에 col 들을 넣기 위한 string
        sql_1001_1500_s = ["%s"] * len(cols_1001_1500)  # sql에 %s 들을 넣기 위한 string
        sql_1001_1500_s = ','.join(map(str, sql_1001_1500_s))
        csv_data_1001_1500 = csv_data_old[cols_1001_1500]
        data_list_1001_1500 = csv_data_1001_1500.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_1001_1500("
        sql += sql_1001_1500_cols
        sql += ") values ("
        sql += sql_1001_1500_s
        sql += ")"
        curs.executemany(sql, data_list_1001_1500)
        conn.commit()
        conn.close()
        print("1001~1500 the end")

    # ITEM 이 2000개 이하일 경우
    elif int(max_item_count) <= 2000:
        index_500 = cols.index("item_500")
        index_1000 = cols.index("item_1000")
        index_1500 = cols.index("item_1500")

        cols_0000_0500 = cols[:index_500 + 1]
        cols_0501_1000 = cols[index_500 + 1:index_1000 + 1]
        cols_1001_1500 = cols[index_1000 + 1:index_1500 + 1]
        cols_1501_2000 = cols[index_1500 + 1:]

        # cols 에 header_uid , date, time 을 추가해야 함.
        cols_0501_1000.append('header_uid')
        cols_0501_1000.append('Date')
        cols_0501_1000.append('Time')

        cols_1001_1500.append('header_uid')
        cols_1001_1500.append('Date')
        cols_1001_1500.append('Time')

        cols_1501_2000.append('header_uid')
        cols_1501_2000.append('Date')
        cols_1501_2000.append('Time')

        # cols_0000_0500
        sql_0000_0500_cols = ','.join(map(str, cols_0000_0500))  # sql 에 col 들을 넣기 위한 string
        sql_0000_0500 = ["%s"] * len(cols_0000_0500)  # sql에 %s 들을 넣기 위한 string
        sql_0000_0500 = ','.join(map(str, sql_0000_0500))
        csv_data_0000_0500 = csv_data_old[cols_0000_0500]
        data_list_0000_0500 = csv_data_0000_0500.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_0000_0500("
        sql += sql_0000_0500_cols
        sql += ") values ("
        sql += sql_0000_0500
        sql += ")"
        curs.executemany(sql, data_list_0000_0500)
        conn.commit()
        conn.close()
        print("0000~0500 the end")

        # 0501~1000
        sql_0501_1000_cols = ','.join(map(str, cols_0501_1000))  # sql 에 col 들을 넣기 위한 string
        sql_0501_1000_s = ["%s"] * len(cols_0501_1000)  # sql에 %s 들을 넣기 위한 string
        sql_0501_1000_s = ','.join(map(str, sql_0501_1000_s))
        csv_data_0501_1000 = csv_data_old[cols_0501_1000]
        data_list_0501_1000 = csv_data_0501_1000.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_0501_1000("
        sql += sql_0501_1000_cols
        sql += ") values ("
        sql += sql_0501_1000_s
        sql += ")"
        curs.executemany(sql, data_list_0501_1000)
        conn.commit()
        conn.close()
        print("0501~1000 the end")

        # 1001~1500
        sql_1001_1500_cols = ','.join(map(str, cols_1001_1500))  # sql 에 col 들을 넣기 위한 string
        sql_1001_1500_s = ["%s"] * len(cols_1001_1500)  # sql에 %s 들을 넣기 위한 string
        sql_1001_1500_s = ','.join(map(str, sql_1001_1500_s))
        csv_data_1001_1500 = csv_data_old[cols_1001_1500]
        data_list_1001_1500 = csv_data_1001_1500.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_1001_1500("
        sql += sql_1001_1500_cols
        sql += ") values ("
        sql += sql_1001_1500_s
        sql += ")"
        curs.executemany(sql, data_list_1001_1500)
        conn.commit()
        conn.close()
        print("1001~1500 the end")

        # 1501~2000
        sql_1501_2000_cols = ','.join(map(str, cols_1501_2000))  # sql 에 col 들을 넣기 위한 string
        sql_1501_2000_s = ["%s"] * len(cols_1501_2000)  # sql에 %s 들을 넣기 위한 string
        sql_1501_2000_s = ','.join(map(str, sql_1501_2000_s))
        csv_data_1501_2000 = csv_data_old[cols_1501_2000]
        data_list_1501_2000 = csv_data_1501_2000.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_1501_2000("
        sql += sql_1501_2000_cols
        sql += ") values ("
        sql += sql_1501_2000_s
        sql += ")"
        curs.executemany(sql, data_list_1501_2000)
        conn.commit()
        conn.close()
        print("1501~2000 the end")

    # ITEM 이 2500개 이하일 경우
    elif int(max_item_count) <= 2500:
        index_500 = cols.index("item_500")
        index_1000 = cols.index("item_1000")
        index_1500 = cols.index("item_1500")
        index_2000 = cols.index("item_2000")

        cols_0000_0500 = cols[:index_500 + 1]
        cols_0501_1000 = cols[index_500 + 1:index_1000 + 1]
        cols_1001_1500 = cols[index_1000 + 1:index_1500 + 1]
        cols_1501_2000 = cols[index_1500 + 1:index_2000 + 1]
        cols_2001_2500 = cols[index_2000 + 1:]

        # cols 에 header_uid , date, time 을 추가해야 함.
        cols_0501_1000.append('header_uid')
        cols_0501_1000.append('Date')
        cols_0501_1000.append('Time')

        cols_1001_1500.append('header_uid')
        cols_1001_1500.append('Date')
        cols_1001_1500.append('Time')

        cols_1501_2000.append('header_uid')
        cols_1501_2000.append('Date')
        cols_1501_2000.append('Time')

        cols_2001_2500.append('header_uid')
        cols_2001_2500.append('Date')
        cols_2001_2500.append('Time')

        # cols_0000_0500
        sql_0000_0500_cols = ','.join(map(str, cols_0000_0500))  # sql 에 col 들을 넣기 위한 string
        sql_0000_0500 = ["%s"] * len(cols_0000_0500)  # sql에 %s 들을 넣기 위한 string
        sql_0000_0500 = ','.join(map(str, sql_0000_0500))
        csv_data_0000_0500 = csv_data_old[cols_0000_0500]
        data_list_0000_0500 = csv_data_0000_0500.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_0000_0500("
        sql += sql_0000_0500_cols
        sql += ") values ("
        sql += sql_0000_0500
        sql += ")"
        curs.executemany(sql, data_list_0000_0500)
        conn.commit()
        conn.close()
        print("0000~0500 the end")

        # 0501~1000
        sql_0501_1000_cols = ','.join(map(str, cols_0501_1000))  # sql 에 col 들을 넣기 위한 string
        sql_0501_1000_s = ["%s"] * len(cols_0501_1000)  # sql에 %s 들을 넣기 위한 string
        sql_0501_1000_s = ','.join(map(str, sql_0501_1000_s))
        csv_data_0501_1000 = csv_data_old[cols_0501_1000]
        data_list_0501_1000 = csv_data_0501_1000.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_0501_1000("
        sql += sql_0501_1000_cols
        sql += ") values ("
        sql += sql_0501_1000_s
        sql += ")"
        curs.executemany(sql, data_list_0501_1000)
        conn.commit()
        conn.close()
        print("0501~1000 the end")

        # 1001~1500
        sql_1001_1500_cols = ','.join(map(str, cols_1001_1500))  # sql 에 col 들을 넣기 위한 string
        sql_1001_1500_s = ["%s"] * len(cols_1001_1500)  # sql에 %s 들을 넣기 위한 string
        sql_1001_1500_s = ','.join(map(str, sql_1001_1500_s))
        csv_data_1001_1500 = csv_data_old[cols_1001_1500]
        data_list_1001_1500 = csv_data_1001_1500.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_1001_1500("
        sql += sql_1001_1500_cols
        sql += ") values ("
        sql += sql_1001_1500_s
        sql += ")"
        curs.executemany(sql, data_list_1001_1500)
        conn.commit()
        conn.close()
        print("1001~1500 the end")

        # 1501~2000
        sql_1501_2000_cols = ','.join(map(str, cols_1501_2000))  # sql 에 col 들을 넣기 위한 string
        sql_1501_2000_s = ["%s"] * len(cols_1501_2000)  # sql에 %s 들을 넣기 위한 string
        sql_1501_2000_s = ','.join(map(str, sql_1501_2000_s))
        csv_data_1501_2000 = csv_data_old[cols_1501_2000]
        data_list_1501_2000 = csv_data_1501_2000.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_1501_2000("
        sql += sql_1501_2000_cols
        sql += ") values ("
        sql += sql_1501_2000_s
        sql += ")"
        curs.executemany(sql, data_list_1501_2000)
        conn.commit()
        conn.close()
        print("1501~2000 the end")

        # 2001~2500
        sql_2001_2500_cols = ','.join(map(str, cols_2001_2500))  # sql 에 col 들을 넣기 위한 string
        sql_2001_2500_s = ["%s"] * len(cols_2001_2500)  # sql에 %s 들을 넣기 위한 string
        sql_2001_2500_s = ','.join(map(str, sql_2001_2500_s))
        csv_data_2001_2500 = csv_data_old[cols_2001_2500]
        data_list_2001_2500 = csv_data_2001_2500.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_2001_2500("
        sql += sql_2001_2500_cols
        sql += ") values ("
        sql += sql_2001_2500_s
        sql += ")"
        curs.executemany(sql, data_list_2001_2500)
        conn.commit()
        conn.close()
        print("2001_2500 the end")

    # ITEM 이 3000개 이하일 경우
    elif int(max_item_count) <= 3000:
        index_500 = cols.index("item_500")
        index_1000 = cols.index("item_1000")
        index_1500 = cols.index("item_1500")
        index_2000 = cols.index("item_2000")
        index_2500 = cols.index("item_2500")

        cols_0000_0500 = cols[:index_500 + 1]
        cols_0501_1000 = cols[index_500 + 1:index_1000 + 1]
        cols_1001_1500 = cols[index_1000 + 1:index_1500 + 1]
        cols_1501_2000 = cols[index_1500 + 1:index_2000 + 1]
        cols_2001_2500 = cols[index_2001 + 1:index_2500 + 1]
        cols_2501_3000 = cols[index_2500 + 1:]

        # cols 에 header_uid , date, time 을 추가해야 함.
        cols_0501_1000.append('header_uid')
        cols_0501_1000.append('Date')
        cols_0501_1000.append('Time')

        cols_1001_1500.append('header_uid')
        cols_1001_1500.append('Date')
        cols_1001_1500.append('Time')

        cols_1501_2000.append('header_uid')
        cols_1501_2000.append('Date')
        cols_1501_2000.append('Time')

        cols_2001_2500.append('header_uid')
        cols_2001_2500.append('Date')
        cols_2001_2500.append('Time')

        cols_2501_3000.append('header_uid')
        cols_2501_3000.append('Date')
        cols_2501_3000.append('Time')

        # cols_0000_0500
        cols_0000_0500 = ','.join(map(str, cols_0000_0500))  # sql 에 col 들을 넣기 위한 string
        cols_0000_0500 = ["%s"] * len(cols_0000_0500)  # sql에 %s 들을 넣기 위한 string
        sql_0000_0500 = ','.join(map(str, sql_0000_0500))
        csv_data_0000_0500 = csv_data_old[cols_0000_0500]
        data_list_0000_0500 = csv_data_0000_0500.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_0000_0500("
        sql += cols_0000_0500
        sql += ") values ("
        sql += sql_0000_0500
        sql += ")"
        curs.executemany(sql, data_list_0000_0500)
        conn.commit()
        conn.close()
        print("0000~0500 the end")

        # 0501~1000
        sql_0501_1000_cols = ','.join(map(str, cols_0501_1000))  # sql 에 col 들을 넣기 위한 string
        sql_0501_1000_s = ["%s"] * len(cols_0501_1000)  # sql에 %s 들을 넣기 위한 string
        sql_0501_1000_s = ','.join(map(str, sql_0501_1000_s))
        csv_data_0501_1000 = csv_data_old[cols_0501_1000]
        data_list_0501_1000 = csv_data_0501_1000.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_0501_1000("
        sql += sql_0501_1000_cols
        sql += ") values ("
        sql += sql_0501_1000_s
        sql += ")"
        curs.executemany(sql, data_list_0501_1000)
        conn.commit()
        conn.close()
        print("0501~1000 the end")

        # 1001~1500
        sql_1001_1500_cols = ','.join(map(str, cols_1001_1500))  # sql 에 col 들을 넣기 위한 string
        sql_1001_1500_s = ["%s"] * len(cols_1001_1500)  # sql에 %s 들을 넣기 위한 string
        sql_1001_1500_s = ','.join(map(str, sql_1001_1500_s))
        csv_data_1001_1500 = csv_data_old[cols_1001_1500]
        data_list_1001_1500 = csv_data_1001_1500.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_1001_1500("
        sql += sql_1001_1500_cols
        sql += ") values ("
        sql += sql_1001_1500_s
        sql += ")"
        curs.executemany(sql, data_list_1001_1500)
        conn.commit()
        conn.close()
        print("1001~1500 the end")

        # 1501~2000
        sql_1501_2000_cols = ','.join(map(str, cols_1501_2000))  # sql 에 col 들을 넣기 위한 string
        sql_1501_2000_s = ["%s"] * len(cols_1501_2000)  # sql에 %s 들을 넣기 위한 string
        sql_1501_2000_s = ','.join(map(str, sql_1501_2000_s))
        csv_data_1501_2000 = csv_data_old[cols_1501_2000]
        data_list_1501_2000 = csv_data_1501_2000.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_1501_2000("
        sql += sql_1501_2000_cols
        sql += ") values ("
        sql += sql_1501_2000_s
        sql += ")"
        curs.executemany(sql, data_list_1501_2000)
        conn.commit()
        conn.close()
        print("1501~2000 the end")

        # 2001~2500
        sql_2001_2500_cols = ','.join(map(str, cols_2001_2500))  # sql 에 col 들을 넣기 위한 string
        sql_2001_2500_s = ["%s"] * len(cols_2001_2500)  # sql에 %s 들을 넣기 위한 string
        sql_2001_2500_s = ','.join(map(str, sql_2001_2500_s))
        csv_data_2001_2500 = csv_data_old[cols_2001_2500]
        data_list_2001_2500 = csv_data_2001_2500.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_2001_2500("
        sql += sql_2001_2500_cols
        sql += ") values ("
        sql += sql_2001_2500_s
        sql += ")"
        curs.executemany(sql, data_list_2001_2500)
        conn.commit()
        conn.close()
        print("2001_2500 the end")

        # 2501~3000
        sql_2501_3000_cols = ','.join(map(str, cols_2501_3000))  # sql 에 col 들을 넣기 위한 string
        sql_2501_3000_s = ["%s"] * len(cols_2501_3000)  # sql에 %s 들을 넣기 위한 string
        sql_2501_3000_s = ','.join(map(str, sql_2501_3000_s))
        csv_data_2501_3000 = csv_data_old[cols_2501_3000]
        data_list_2501_3000 = csv_data_2501_3000.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_2501_3000("
        sql += sql_2501_3000_cols
        sql += ") values ("
        sql += sql_2501_3000_s
        sql += ")"
        curs.executemany(sql, data_list_2501_3000)
        conn.commit()
        conn.close()
        print("2501~3000 the end")


    # ITEM 이 3500개 이하일 경우
    elif int(max_item_count) <= 3500:
        index_500 = cols.index("item_500")
        index_1000 = cols.index("item_1000")
        index_1500 = cols.index("item_1500")
        index_2000 = cols.index("item_2000")
        index_2500 = cols.index("item_2500")
        index_3000 = cols.index("item_3000")

        cols_0000_0500 = cols[:index_500 + 1]
        cols_0501_1000 = cols[index_500 + 1:index_1000 + 1]
        cols_1001_1500 = cols[index_1000 + 1:index_1500 + 1]
        cols_1501_2000 = cols[index_1500 + 1:index_2000 + 1]
        cols_2001_2500 = cols[index_2001 + 1:index_2500 + 1]
        cols_2501_3000 = cols[index_2500 + 1:index_3000 + 1]
        cols_3001_3500 = cols[index_3000 + 1:]

        # cols 에 header_uid , date, time 을 추가해야 함.
        cols_0501_1000.append('header_uid')
        cols_0501_1000.append('Date')
        cols_0501_1000.append('Time')

        cols_1001_1500.append('header_uid')
        cols_1001_1500.append('Date')
        cols_1001_1500.append('Time')

        cols_1501_2000.append('header_uid')
        cols_1501_2000.append('Date')
        cols_1501_2000.append('Time')

        cols_2001_2500.append('header_uid')
        cols_2001_2500.append('Date')
        cols_2001_2500.append('Time')

        cols_2501_3000.append('header_uid')
        cols_2501_3000.append('Date')
        cols_2501_3000.append('Time')

        cols_3001_3500.append('header_uid')
        cols_3001_3500.append('Date')
        cols_3001_3500.append('Time')

        # cols_0000_0500
        cols_0000_0500 = ','.join(map(str, cols_0000_0500))  # sql 에 col 들을 넣기 위한 string
        cols_0000_0500 = ["%s"] * len(cols_0000_0500)  # sql에 %s 들을 넣기 위한 string
        sql_0000_0500 = ','.join(map(str, sql_0000_0500))
        csv_data_0000_0500 = csv_data_old[cols_0000_0500]
        data_list_0000_0500 = csv_data_0000_0500.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_0000_0500("
        sql += cols_0000_0500
        sql += ") values ("
        sql += sql_0000_0500
        sql += ")"
        curs.executemany(sql, data_list_0000_0500)
        conn.commit()
        conn.close()
        print("0000~0500 the end")

        # 0501~1000
        sql_0501_1000_cols = ','.join(map(str, cols_0501_1000))  # sql 에 col 들을 넣기 위한 string
        sql_0501_1000_s = ["%s"] * len(cols_0501_1000)  # sql에 %s 들을 넣기 위한 string
        sql_0501_1000_s = ','.join(map(str, sql_0501_1000_s))
        csv_data_0501_1000 = csv_data_old[cols_0501_1000]
        data_list_0501_1000 = csv_data_0501_1000.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_0501_1000("
        sql += sql_0501_1000_cols
        sql += ") values ("
        sql += sql_0501_1000_s
        sql += ")"
        curs.executemany(sql, data_list_0501_1000)
        conn.commit()
        conn.close()
        print("0501~1000 the end")

        # 1001~1500
        sql_1001_1500_cols = ','.join(map(str, cols_1001_1500))  # sql 에 col 들을 넣기 위한 string
        sql_1001_1500_s = ["%s"] * len(cols_1001_1500)  # sql에 %s 들을 넣기 위한 string
        sql_1001_1500_s = ','.join(map(str, sql_1001_1500_s))
        csv_data_1001_1500 = csv_data_old[cols_1001_1500]
        data_list_1001_1500 = csv_data_1001_1500.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_1001_1500("
        sql += sql_1001_1500_cols
        sql += ") values ("
        sql += sql_1001_1500_s
        sql += ")"
        curs.executemany(sql, data_list_1001_1500)
        conn.commit()
        conn.close()
        print("1001~1500 the end")

        # 1501~2000
        sql_1501_2000_cols = ','.join(map(str, cols_1501_2000))  # sql 에 col 들을 넣기 위한 string
        sql_1501_2000_s = ["%s"] * len(cols_1501_2000)  # sql에 %s 들을 넣기 위한 string
        sql_1501_2000_s = ','.join(map(str, sql_1501_2000_s))
        csv_data_1501_2000 = csv_data_old[cols_1501_2000]
        data_list_1501_2000 = csv_data_1501_2000.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_1501_2000("
        sql += sql_1501_2000_cols
        sql += ") values ("
        sql += sql_1501_2000_s
        sql += ")"
        curs.executemany(sql, data_list_1501_2000)
        conn.commit()
        conn.close()
        print("1501~2000 the end")

        # 2001~2500
        sql_2001_2500_cols = ','.join(map(str, cols_2001_2500))  # sql 에 col 들을 넣기 위한 string
        sql_2001_2500_s = ["%s"] * len(cols_2001_2500)  # sql에 %s 들을 넣기 위한 string
        sql_2001_2500_s = ','.join(map(str, sql_2001_2500_s))
        csv_data_2001_2500 = csv_data_old[cols_2001_2500]
        data_list_2001_2500 = csv_data_2001_2500.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_2001_2500("
        sql += sql_2001_2500_cols
        sql += ") values ("
        sql += sql_2001_2500_s
        sql += ")"
        curs.executemany(sql, data_list_2001_2500)
        conn.commit()
        conn.close()
        print("2001_2500 the end")

        # 2501~3000
        sql_2501_3000_cols = ','.join(map(str, cols_2501_3000))  # sql 에 col 들을 넣기 위한 string
        sql_2501_3000_s = ["%s"] * len(cols_2501_3000)  # sql에 %s 들을 넣기 위한 string
        sql_2501_3000_s = ','.join(map(str, sql_2501_3000_s))
        csv_data_2501_3000 = csv_data_old[cols_2501_3000]
        data_list_2501_3000 = csv_data_2501_3000.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_2501_3000("
        sql += sql_2501_3000_cols
        sql += ") values ("
        sql += sql_2501_3000_s
        sql += ")"
        curs.executemany(sql, data_list_2501_3000)
        conn.commit()
        conn.close()
        print("2501~3000 the end")

        # 3001~3500
        sql_3001_3500_cols = ','.join(map(str, cols_3001_3500))  # sql 에 col 들을 넣기 위한 string
        sql_3001_3500_s = ["%s"] * len(cols_3001_3500)  # sql에 %s 들을 넣기 위한 string
        sql_3001_3500_s = ','.join(map(str, sql_3001_3500_s))
        csv_data_3001_3500 = csv_data_old[cols_3001_3500]
        data_list_3001_3500 = csv_data_3001_3500.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_3001_3500("
        sql += sql_3001_3500_cols
        sql += ") values ("
        sql += sql_3001_3500_s
        sql += ")"
        curs.executemany(sql, data_list_3001_3500)
        conn.commit()
        conn.close()
        print("3001~3500 the end")


    # ITEM 이 4000개 이하일 경우
    elif int(max_item_count) <= 4000:
        index_500 = cols.index("item_500")
        index_1000 = cols.index("item_1000")
        index_1500 = cols.index("item_1500")
        index_2000 = cols.index("item_2000")
        index_2500 = cols.index("item_2500")
        index_3000 = cols.index("item_3000")
        index_3500 = cols.index("item_3500")

        cols_0000_0500 = cols[:index_500 + 1]
        cols_0501_1000 = cols[index_500 + 1:index_1000 + 1]
        cols_1001_1500 = cols[index_1000 + 1:index_1500 + 1]
        cols_1501_2000 = cols[index_1500 + 1:index_2000 + 1]
        cols_2001_2500 = cols[index_2001 + 1:index_2500 + 1]
        cols_2501_3000 = cols[index_2500 + 1:index_3000 + 1]
        cols_3001_3500 = cols[index_3000 + 1:index_3500 + 1]
        cols_3501_4000 = cols[index_3500 + 1:]

        # cols 에 header_uid , date, time 을 추가해야 함.
        cols_0501_1000.append('header_uid')
        cols_0501_1000.append('Date')
        cols_0501_1000.append('Time')

        cols_1001_1500.append('header_uid')
        cols_1001_1500.append('Date')
        cols_1001_1500.append('Time')

        cols_1501_2000.append('header_uid')
        cols_1501_2000.append('Date')
        cols_1501_2000.append('Time')

        cols_2001_2500.append('header_uid')
        cols_2001_2500.append('Date')
        cols_2001_2500.append('Time')

        cols_2501_3000.append('header_uid')
        cols_2501_3000.append('Date')
        cols_2501_3000.append('Time')

        cols_3001_3500.append('header_uid')
        cols_3001_3500.append('Date')
        cols_3001_3500.append('Time')

        cols_3501_4000.append('header_uid')
        cols_3501_4000.append('Date')
        cols_3501_4000.append('Time')

        # cols_0000_0500
        cols_0000_0500 = ','.join(map(str, cols_0000_0500))  # sql 에 col 들을 넣기 위한 string
        cols_0000_0500 = ["%s"] * len(cols_0000_0500)  # sql에 %s 들을 넣기 위한 string
        sql_0000_0500 = ','.join(map(str, sql_0000_0500))
        csv_data_0000_0500 = csv_data_old[cols_0000_0500]
        data_list_0000_0500 = csv_data_0000_0500.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_0000_0500("
        sql += cols_0000_0500
        sql += ") values ("
        sql += sql_0000_0500
        sql += ")"
        curs.executemany(sql, data_list_0000_0500)
        conn.commit()
        conn.close()
        print("0000~0500 the end")

        # 0501~1000
        sql_0501_1000_cols = ','.join(map(str, cols_0501_1000))  # sql 에 col 들을 넣기 위한 string
        sql_0501_1000_s = ["%s"] * len(cols_0501_1000)  # sql에 %s 들을 넣기 위한 string
        sql_0501_1000_s = ','.join(map(str, sql_0501_1000_s))
        csv_data_0501_1000 = csv_data_old[cols_0501_1000]
        data_list_0501_1000 = csv_data_0501_1000.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_0501_1000("
        sql += sql_0501_1000_cols
        sql += ") values ("
        sql += sql_0501_1000_s
        sql += ")"
        curs.executemany(sql, data_list_0501_1000)
        conn.commit()
        conn.close()
        print("0501~1000 the end")

        # 1001~1500
        sql_1001_1500_cols = ','.join(map(str, cols_1001_1500))  # sql 에 col 들을 넣기 위한 string
        sql_1001_1500_s = ["%s"] * len(cols_1001_1500)  # sql에 %s 들을 넣기 위한 string
        sql_1001_1500_s = ','.join(map(str, sql_1001_1500_s))
        csv_data_1001_1500 = csv_data_old[cols_1001_1500]
        data_list_1001_1500 = csv_data_1001_1500.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_1001_1500("
        sql += sql_1001_1500_cols
        sql += ") values ("
        sql += sql_1001_1500_s
        sql += ")"
        curs.executemany(sql, data_list_1001_1500)
        conn.commit()
        conn.close()
        print("1001~1500 the end")

        # 1501~2000
        sql_1501_2000_cols = ','.join(map(str, cols_1501_2000))  # sql 에 col 들을 넣기 위한 string
        sql_1501_2000_s = ["%s"] * len(cols_1501_2000)  # sql에 %s 들을 넣기 위한 string
        sql_1501_2000_s = ','.join(map(str, sql_1501_2000_s))
        csv_data_1501_2000 = csv_data_old[cols_1501_2000]
        data_list_1501_2000 = csv_data_1501_2000.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_1501_2000("
        sql += sql_1501_2000_cols
        sql += ") values ("
        sql += sql_1501_2000_s
        sql += ")"
        curs.executemany(sql, data_list_1501_2000)
        conn.commit()
        conn.close()
        print("1501~2000 the end")

        # 2001~2500
        sql_2001_2500_cols = ','.join(map(str, cols_2001_2500))  # sql 에 col 들을 넣기 위한 string
        sql_2001_2500_s = ["%s"] * len(cols_2001_2500)  # sql에 %s 들을 넣기 위한 string
        sql_2001_2500_s = ','.join(map(str, sql_2001_2500_s))
        csv_data_2001_2500 = csv_data_old[cols_2001_2500]
        data_list_2001_2500 = csv_data_2001_2500.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_2001_2500("
        sql += sql_2001_2500_cols
        sql += ") values ("
        sql += sql_2001_2500_s
        sql += ")"
        curs.executemany(sql, data_list_2001_2500)
        conn.commit()
        conn.close()
        print("2001_2500 the end")

        # 2501~3000
        sql_2501_3000_cols = ','.join(map(str, cols_2501_3000))  # sql 에 col 들을 넣기 위한 string
        sql_2501_3000_s = ["%s"] * len(cols_2501_3000)  # sql에 %s 들을 넣기 위한 string
        sql_2501_3000_s = ','.join(map(str, sql_2501_3000_s))
        csv_data_2501_3000 = csv_data_old[cols_2501_3000]
        data_list_2501_3000 = csv_data_2501_3000.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_2501_3000("
        sql += sql_2501_3000_cols
        sql += ") values ("
        sql += sql_2501_3000_s
        sql += ")"
        curs.executemany(sql, data_list_2501_3000)
        conn.commit()
        conn.close()
        print("2501~3000 the end")

        # 3001~3500
        sql_3001_3500_cols = ','.join(map(str, cols_3001_3500))  # sql 에 col 들을 넣기 위한 string
        sql_3001_3500_s = ["%s"] * len(cols_3001_3500)  # sql에 %s 들을 넣기 위한 string
        sql_3001_3500_s = ','.join(map(str, sql_3001_3500_s))
        csv_data_3001_3500 = csv_data_old[cols_3001_3500]
        data_list_3001_3500 = csv_data_3001_3500.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_3001_3500("
        sql += sql_3001_3500_cols
        sql += ") values ("
        sql += sql_3001_3500_s
        sql += ")"
        curs.executemany(sql, data_list_3001_3500)
        conn.commit()
        conn.close()
        print("3001~3500 the end")


        # 3501~4000
        sql_3501_4000_cols = ','.join(map(str, cols_3501_4000))  # sql 에 col 들을 넣기 위한 string
        sql_3501_4000_s = ["%s"] * len(cols_3501_4000)  # sql에 %s 들을 넣기 위한 string
        sql_3501_4000_s = ','.join(map(str, sql_3501_4000_s))
        csv_data_3501_4000 = csv_data_old[cols_3501_4000]
        data_list_3501_4000 = csv_data_3501_4000.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_3501_4000("
        sql += sql_3501_4000_cols
        sql += ") values ("
        sql += sql_3501_4000_s
        sql += ")"
        curs.executemany(sql, data_list_3501_4000)
        conn.commit()
        conn.close()
        print("3501~4000 the end")


    # ITEM 이 4500개 이하일 경우
    elif int(max_item_count) <= 4500:
        index_500 = cols.index("item_500")
        index_1000 = cols.index("item_1000")
        index_1500 = cols.index("item_1500")
        index_2000 = cols.index("item_2000")
        index_2500 = cols.index("item_2500")
        index_3000 = cols.index("item_3000")
        index_3500 = cols.index("item_3500")
        index_4000 = cols.index("item_4000")

        cols_0000_0500 = cols[:index_500 + 1]
        cols_0501_1000 = cols[index_500 + 1:index_1000 + 1]
        cols_1001_1500 = cols[index_1000 + 1:index_1500 + 1]
        cols_1501_2000 = cols[index_1500 + 1:index_2000 + 1]
        cols_2001_2500 = cols[index_2001 + 1:index_2500 + 1]
        cols_2501_3000 = cols[index_2500 + 1:index_3000 + 1]
        cols_3001_3500 = cols[index_3000 + 1:index_3500 + 1]
        cols_3501_4000 = cols[index_3500 + 1:index_4000 + 1]
        cols_4001_4500 = cols[index_4000 + 1:]

        # cols 에 header_uid , date, time 을 추가해야 함.
        cols_0501_1000.append('header_uid')
        cols_0501_1000.append('Date')
        cols_0501_1000.append('Time')

        cols_1001_1500.append('header_uid')
        cols_1001_1500.append('Date')
        cols_1001_1500.append('Time')

        cols_1501_2000.append('header_uid')
        cols_1501_2000.append('Date')
        cols_1501_2000.append('Time')

        cols_2001_2500.append('header_uid')
        cols_2001_2500.append('Date')
        cols_2001_2500.append('Time')

        cols_2501_3000.append('header_uid')
        cols_2501_3000.append('Date')
        cols_2501_3000.append('Time')

        cols_3001_3500.append('header_uid')
        cols_3001_3500.append('Date')
        cols_3001_3500.append('Time')

        cols_3501_4000.append('header_uid')
        cols_3501_4000.append('Date')
        cols_3501_4000.append('Time')

        cols_4001_4500.append('header_uid')
        cols_4001_4500.append('Date')
        cols_4001_4500.append('Time')

        # cols_0000_0500
        cols_0000_0500 = ','.join(map(str, cols_0000_0500))  # sql 에 col 들을 넣기 위한 string
        cols_0000_0500 = ["%s"] * len(cols_0000_0500)  # sql에 %s 들을 넣기 위한 string
        sql_0000_0500 = ','.join(map(str, sql_0000_0500))
        csv_data_0000_0500 = csv_data_old[cols_0000_0500]
        data_list_0000_0500 = csv_data_0000_0500.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_0000_0500("
        sql += cols_0000_0500
        sql += ") values ("
        sql += sql_0000_0500
        sql += ")"
        curs.executemany(sql, data_list_0000_0500)
        conn.commit()
        conn.close()
        print("0000~0500 the end")

        # 0501~1000
        sql_0501_1000_cols = ','.join(map(str, cols_0501_1000))  # sql 에 col 들을 넣기 위한 string
        sql_0501_1000_s = ["%s"] * len(cols_0501_1000)  # sql에 %s 들을 넣기 위한 string
        sql_0501_1000_s = ','.join(map(str, sql_0501_1000_s))
        csv_data_0501_1000 = csv_data_old[cols_0501_1000]
        data_list_0501_1000 = csv_data_0501_1000.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_0501_1000("
        sql += sql_0501_1000_cols
        sql += ") values ("
        sql += sql_0501_1000_s
        sql += ")"
        curs.executemany(sql, data_list_0501_1000)
        conn.commit()
        conn.close()
        print("0501~1000 the end")

        # 1001~1500
        sql_1001_1500_cols = ','.join(map(str, cols_1001_1500))  # sql 에 col 들을 넣기 위한 string
        sql_1001_1500_s = ["%s"] * len(cols_1001_1500)  # sql에 %s 들을 넣기 위한 string
        sql_1001_1500_s = ','.join(map(str, sql_1001_1500_s))
        csv_data_1001_1500 = csv_data_old[cols_1001_1500]
        data_list_1001_1500 = csv_data_1001_1500.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_1001_1500("
        sql += sql_1001_1500_cols
        sql += ") values ("
        sql += sql_1001_1500_s
        sql += ")"
        curs.executemany(sql, data_list_1001_1500)
        conn.commit()
        conn.close()
        print("1001~1500 the end")

        # 1501~2000
        sql_1501_2000_cols = ','.join(map(str, cols_1501_2000))  # sql 에 col 들을 넣기 위한 string
        sql_1501_2000_s = ["%s"] * len(cols_1501_2000)  # sql에 %s 들을 넣기 위한 string
        sql_1501_2000_s = ','.join(map(str, sql_1501_2000_s))
        csv_data_1501_2000 = csv_data_old[cols_1501_2000]
        data_list_1501_2000 = csv_data_1501_2000.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_1501_2000("
        sql += sql_1501_2000_cols
        sql += ") values ("
        sql += sql_1501_2000_s
        sql += ")"
        curs.executemany(sql, data_list_1501_2000)
        conn.commit()
        conn.close()
        print("1501~2000 the end")

        # 2001~2500
        sql_2001_2500_cols = ','.join(map(str, cols_2001_2500))  # sql 에 col 들을 넣기 위한 string
        sql_2001_2500_s = ["%s"] * len(cols_2001_2500)  # sql에 %s 들을 넣기 위한 string
        sql_2001_2500_s = ','.join(map(str, sql_2001_2500_s))
        csv_data_2001_2500 = csv_data_old[cols_2001_2500]
        data_list_2001_2500 = csv_data_2001_2500.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_2001_2500("
        sql += sql_2001_2500_cols
        sql += ") values ("
        sql += sql_2001_2500_s
        sql += ")"
        curs.executemany(sql, data_list_2001_2500)
        conn.commit()
        conn.close()
        print("2001_2500 the end")

        # 2501~3000
        sql_2501_3000_cols = ','.join(map(str, cols_2501_3000))  # sql 에 col 들을 넣기 위한 string
        sql_2501_3000_s = ["%s"] * len(cols_2501_3000)  # sql에 %s 들을 넣기 위한 string
        sql_2501_3000_s = ','.join(map(str, sql_2501_3000_s))
        csv_data_2501_3000 = csv_data_old[cols_2501_3000]
        data_list_2501_3000 = csv_data_2501_3000.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_2501_3000("
        sql += sql_2501_3000_cols
        sql += ") values ("
        sql += sql_2501_3000_s
        sql += ")"
        curs.executemany(sql, data_list_2501_3000)
        conn.commit()
        conn.close()
        print("2501~3000 the end")

        # 3001~3500
        sql_3001_3500_cols = ','.join(map(str, cols_3001_3500))  # sql 에 col 들을 넣기 위한 string
        sql_3001_3500_s = ["%s"] * len(cols_3001_3500)  # sql에 %s 들을 넣기 위한 string
        sql_3001_3500_s = ','.join(map(str, sql_3001_3500_s))
        csv_data_3001_3500 = csv_data_old[cols_3001_3500]
        data_list_3001_3500 = csv_data_3001_3500.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_3001_3500("
        sql += sql_3001_3500_cols
        sql += ") values ("
        sql += sql_3001_3500_s
        sql += ")"
        curs.executemany(sql, data_list_3001_3500)
        conn.commit()
        conn.close()
        print("3001~3500 the end")

        # 3501~4000
        sql_3501_4000_cols = ','.join(map(str, cols_3501_4000))  # sql 에 col 들을 넣기 위한 string
        sql_3501_4000_s = ["%s"] * len(cols_3501_4000)  # sql에 %s 들을 넣기 위한 string
        sql_3501_4000_s = ','.join(map(str, sql_3501_4000_s))
        csv_data_3501_4000 = csv_data_old[cols_3501_4000]
        data_list_3501_4000 = csv_data_3501_4000.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_3501_4000("
        sql += sql_3501_4000_cols
        sql += ") values ("
        sql += sql_3501_4000_s
        sql += ")"
        curs.executemany(sql, data_list_3501_4000)
        conn.commit()
        conn.close()
        print("3501~4000 the end")


        # 4001~4500
        sql_4001_4500_cols = ','.join(map(str, cols_4001_4500))  # sql 에 col 들을 넣기 위한 string
        sql_4001_4500_s = ["%s"] * len(cols_4001_4500)  # sql에 %s 들을 넣기 위한 string
        sql_4001_4500_s = ','.join(map(str, sql_4001_4500_s))
        csv_data_4001_4500 = csv_data_old[cols_4001_4500]
        data_list_4001_4500 = csv_data_4001_4500.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_4001_4500("
        sql += sql_4001_4500_cols
        sql += ") values ("
        sql += sql_4001_4500_s
        sql += ")"
        curs.executemany(sql, data_list_4001_4500)
        conn.commit()
        conn.close()
        print("4501~4500 the end")

    # ITEM 이 5000개 이하일 경우
    elif int(max_item_count) <= 5000:
        index_500 = cols.index("item_500")
        index_1000 = cols.index("item_1000")
        index_1500 = cols.index("item_1500")
        index_2000 = cols.index("item_2000")
        index_2500 = cols.index("item_2500")
        index_3000 = cols.index("item_3000")
        index_3500 = cols.index("item_3500")
        index_4000 = cols.index("item_4000")
        index_4500 = cols.index("item_4500")

        cols_0000_0500 = cols[:index_500 + 1]
        cols_0501_1000 = cols[index_500 + 1:index_1000 + 1]
        cols_1001_1500 = cols[index_1000 + 1:index_1500 + 1]
        cols_1501_2000 = cols[index_1500 + 1:index_2000 + 1]
        cols_2001_2500 = cols[index_2001 + 1:index_2500 + 1]
        cols_2501_3000 = cols[index_2500 + 1:index_3000 + 1]
        cols_3001_3500 = cols[index_3000 + 1:index_3500 + 1]
        cols_3501_4000 = cols[index_3500 + 1:index_4000 + 1]
        cols_4001_4500 = cols[index_4000 + 1:index_4500 + 1]
        cols_4501_5000 = cols[index_4500 + 1:]

        # cols 에 header_uid , date, time 을 추가해야 함.
        cols_0501_1000.append('header_uid')
        cols_0501_1000.append('Date')
        cols_0501_1000.append('Time')

        cols_1001_1500.append('header_uid')
        cols_1001_1500.append('Date')
        cols_1001_1500.append('Time')

        cols_1501_2000.append('header_uid')
        cols_1501_2000.append('Date')
        cols_1501_2000.append('Time')

        cols_2001_2500.append('header_uid')
        cols_2001_2500.append('Date')
        cols_2001_2500.append('Time')

        cols_2501_3000.append('header_uid')
        cols_2501_3000.append('Date')
        cols_2501_3000.append('Time')

        cols_3001_3500.append('header_uid')
        cols_3001_3500.append('Date')
        cols_3001_3500.append('Time')

        cols_3501_4000.append('header_uid')
        cols_3501_4000.append('Date')
        cols_3501_4000.append('Time')

        cols_4001_4500.append('header_uid')
        cols_4001_4500.append('Date')
        cols_4001_4500.append('Time')

        cols_4501_5000.append('header_uid')
        cols_4501_5000.append('Date')
        cols_4501_5000.append('Time')

        # cols_0000_0500
        cols_0000_0500 = ','.join(map(str, cols_0000_0500))  # sql 에 col 들을 넣기 위한 string
        cols_0000_0500 = ["%s"] * len(cols_0000_0500)  # sql에 %s 들을 넣기 위한 string
        sql_0000_0500 = ','.join(map(str, sql_0000_0500))
        csv_data_0000_0500 = csv_data_old[cols_0000_0500]
        data_list_0000_0500 = csv_data_0000_0500.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_0000_0500("
        sql += cols_0000_0500
        sql += ") values ("
        sql += sql_0000_0500
        sql += ")"
        curs.executemany(sql, data_list_0000_0500)
        conn.commit()
        conn.close()
        print("0000~0500 the end")

        # 0501~1000
        sql_0501_1000_cols = ','.join(map(str, cols_0501_1000))  # sql 에 col 들을 넣기 위한 string
        sql_0501_1000_s = ["%s"] * len(cols_0501_1000)  # sql에 %s 들을 넣기 위한 string
        sql_0501_1000_s = ','.join(map(str, sql_0501_1000_s))
        csv_data_0501_1000 = csv_data_old[cols_0501_1000]
        data_list_0501_1000 = csv_data_0501_1000.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_0501_1000("
        sql += sql_0501_1000_cols
        sql += ") values ("
        sql += sql_0501_1000_s
        sql += ")"
        curs.executemany(sql, data_list_0501_1000)
        conn.commit()
        conn.close()
        print("0501~1000 the end")

        # 1001~1500
        sql_1001_1500_cols = ','.join(map(str, cols_1001_1500))  # sql 에 col 들을 넣기 위한 string
        sql_1001_1500_s = ["%s"] * len(cols_1001_1500)  # sql에 %s 들을 넣기 위한 string
        sql_1001_1500_s = ','.join(map(str, sql_1001_1500_s))
        csv_data_1001_1500 = csv_data_old[cols_1001_1500]
        data_list_1001_1500 = csv_data_1001_1500.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_1001_1500("
        sql += sql_1001_1500_cols
        sql += ") values ("
        sql += sql_1001_1500_s
        sql += ")"
        curs.executemany(sql, data_list_1001_1500)
        conn.commit()
        conn.close()
        print("1001~1500 the end")

        # 1501~2000
        sql_1501_2000_cols = ','.join(map(str, cols_1501_2000))  # sql 에 col 들을 넣기 위한 string
        sql_1501_2000_s = ["%s"] * len(cols_1501_2000)  # sql에 %s 들을 넣기 위한 string
        sql_1501_2000_s = ','.join(map(str, sql_1501_2000_s))
        csv_data_1501_2000 = csv_data_old[cols_1501_2000]
        data_list_1501_2000 = csv_data_1501_2000.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_1501_2000("
        sql += sql_1501_2000_cols
        sql += ") values ("
        sql += sql_1501_2000_s
        sql += ")"
        curs.executemany(sql, data_list_1501_2000)
        conn.commit()
        conn.close()
        print("1501~2000 the end")

        # 2001~2500
        sql_2001_2500_cols = ','.join(map(str, cols_2001_2500))  # sql 에 col 들을 넣기 위한 string
        sql_2001_2500_s = ["%s"] * len(cols_2001_2500)  # sql에 %s 들을 넣기 위한 string
        sql_2001_2500_s = ','.join(map(str, sql_2001_2500_s))
        csv_data_2001_2500 = csv_data_old[cols_2001_2500]
        data_list_2001_2500 = csv_data_2001_2500.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_2001_2500("
        sql += sql_2001_2500_cols
        sql += ") values ("
        sql += sql_2001_2500_s
        sql += ")"
        curs.executemany(sql, data_list_2001_2500)
        conn.commit()
        conn.close()
        print("2001_2500 the end")

        # 2501~3000
        sql_2501_3000_cols = ','.join(map(str, cols_2501_3000))  # sql 에 col 들을 넣기 위한 string
        sql_2501_3000_s = ["%s"] * len(cols_2501_3000)  # sql에 %s 들을 넣기 위한 string
        sql_2501_3000_s = ','.join(map(str, sql_2501_3000_s))
        csv_data_2501_3000 = csv_data_old[cols_2501_3000]
        data_list_2501_3000 = csv_data_2501_3000.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_2501_3000("
        sql += sql_2501_3000_cols
        sql += ") values ("
        sql += sql_2501_3000_s
        sql += ")"
        curs.executemany(sql, data_list_2501_3000)
        conn.commit()
        conn.close()
        print("2501~3000 the end")

        # 3001~3500
        sql_3001_3500_cols = ','.join(map(str, cols_3001_3500))  # sql 에 col 들을 넣기 위한 string
        sql_3001_3500_s = ["%s"] * len(cols_3001_3500)  # sql에 %s 들을 넣기 위한 string
        sql_3001_3500_s = ','.join(map(str, sql_3001_3500_s))
        csv_data_3001_3500 = csv_data_old[cols_3001_3500]
        data_list_3001_3500 = csv_data_3001_3500.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_3001_3500("
        sql += sql_3001_3500_cols
        sql += ") values ("
        sql += sql_3001_3500_s
        sql += ")"
        curs.executemany(sql, data_list_3001_3500)
        conn.commit()
        conn.close()
        print("3001~3500 the end")

        # 3501~4000
        sql_3501_4000_cols = ','.join(map(str, cols_3501_4000))  # sql 에 col 들을 넣기 위한 string
        sql_3501_4000_s = ["%s"] * len(cols_3501_4000)  # sql에 %s 들을 넣기 위한 string
        sql_3501_4000_s = ','.join(map(str, sql_3501_4000_s))
        csv_data_3501_4000 = csv_data_old[cols_3501_4000]
        data_list_3501_4000 = csv_data_3501_4000.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_3501_4000("
        sql += sql_3501_4000_cols
        sql += ") values ("
        sql += sql_3501_4000_s
        sql += ")"
        curs.executemany(sql, data_list_3501_4000)
        conn.commit()
        conn.close()
        print("3501~4000 the end")


        # 4001~4500
        sql_4001_4500_cols = ','.join(map(str, cols_4001_4500))  # sql 에 col 들을 넣기 위한 string
        sql_4001_4500_s = ["%s"] * len(cols_4001_4500)  # sql에 %s 들을 넣기 위한 string
        sql_4001_4500_s = ','.join(map(str, sql_4001_4500_s))
        csv_data_4001_4500 = csv_data_old[cols_4001_4500]
        data_list_4001_4500 = csv_data_4001_4500.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_4001_4500("
        sql += sql_4001_4500_cols
        sql += ") values ("
        sql += sql_4001_4500_s
        sql += ")"
        curs.executemany(sql, data_list_4001_4500)
        conn.commit()
        conn.close()
        print("4001~4500 the end")

        # 4501~5000
        sql_4501_5000_cols = ','.join(map(str, cols_4501_5000))  # sql 에 col 들을 넣기 위한 string
        sql_4501_5000_s = ["%s"] * len(cols_4501_5000)  # sql에 %s 들을 넣기 위한 string
        sql_4501_5000_s = ','.join(map(str, sql_4501_5000_s))
        csv_data_4501_5000 = csv_data_old[cols_4501_5000]
        data_list_4501_5000 = csv_data_4501_5000.values.tolist()
        conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                               db='lg_work', charset='utf8')
        curs = conn.cursor()
        sql = "insert into raw_4501_5000("
        sql += sql_4501_5000_cols
        sql += ") values ("
        sql += sql_4501_5000_s
        sql += ")"
        curs.executemany(sql, data_list_4501_5000)
        conn.commit()
        conn.close()
        print("4501~5000 the end")

# class
class Today_file():

    def __init__(self, filename, time):
        self.filename = filename
        self.time = time

    def __getitem__(self, item):
        print(str(item))

    def __str__(self):
        return "'filename': '{}', 'time': '{}'".format(self.filename, self.time)


# main
path = "C:\\pythontest\\*"
file_move_path = "C:\\after_db_insert_lg\\"
src = "C:\\pythontest\\"
file_list = glob.glob(path)
file_list_csv = [files for files in file_list]
today_files = []

for file in file_list_csv:
    mtime = os.path.getmtime(file)
    file_last_edit_time = datetime.fromtimestamp(mtime)
    temp = Today_file(file, str(file)[-19:].replace("_", ""))
    today_files.append(temp)


# 시간 기준으로 배열 오름차순 정렬
today_files.sort(key=lambda today_file: today_file.time)
for i in range(0, len(today_files)):
    _name = today_files[i].filename
    print(os.path.basename(_name))
    _time = str(today_files[i].time[0:4] + "-" + str(today_files[i].time[4:6]) + "-" + str(today_files[i].time[6:8]) +
                "-" + str(today_files[i].time[8:-4]))
    # header 파일 처리
    if 'header' in _name:
        try:
            insert_head_data(_name, _time)
            shutil.move(src + os.path.basename(_name), file_move_path + os.path.basename(_name))
        except pymysql.err.OperationalError:
            print(_name + u"처리가 제대로 되지 않았습니다.")
    if 'item' in _name:
        try:
            conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                                   db='lg_work', charset='utf8')
            curs = conn.cursor()
            sql = "select * from header where lgmv_date=%s order by header_uid DESC"
            curs.execute(sql, _time)
            rows = curs.fetchall()
            header_uid = rows[0][0]
            conn.commit()
            conn.close()
            insert_item_data(_name, _time, header_uid)
            shutil.move(src + os.path.basename(_name), file_move_path + os.path.basename(_name))
        except pymysql.err.OperationalError:
            print(_name + u"처리가 제대로 되지 않았습니다. 다시 실행해주세요.")
    if 'raw' in _name:
        try:
            conn = pymysql.connect(host='164.125.70.12', user='lg', password='selab',
                                   db='lg_work', charset='utf8')
            curs = conn.cursor()
            sql = "select * from header where lgmv_date=%s order by header_uid DESC"
            curs.execute(sql, _time)
            rows = curs.fetchall()
            header_uid = rows[0][0]
            print("header ID : " + str(header_uid))
            conn.commit()
            conn.close()
            insert_raw_data(_name, _time, header_uid)
            shutil.move(src + os.path.basename(_name), file_move_path + os.path.basename(_name))
        except pymysql.err.OperationalError:
            print(_name + u"처리가 제대로 되지 않았습니다. 다시 실행해주세요.")