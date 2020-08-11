# 定时任务
# crontab -e

# Fissler
import pandas as pd
import sqlalchemy
import datetime

teradata_engine = sqlalchemy.create_engine('teradatasql://chnccp_db_u28074879:Metro@072020@164.61.235.21/')
postgre_engine = sqlalchemy.create_engine('postgresql://metro@10.250.131.75:1526/metabase')

sql = '''delete from chnccp_crm.fissler_report'''
teradata_engine.execute(sql)

query = '''select pick_store, create_time::timestamp without time zone, goods_outer_id, goods_count, goods_name from cm_prod.report_data
where campaign_id = 13
'''
df = pd.read_sql(query, postgre_engine)
df.to_sql('fissler_report', schema = 'chnccp_crm', con = teradata_engine, if_exists='append', index = False )

print('Task Finisehd')
print(datetime.datetime.now())


50 08 * * * python /opt/ganyu/fissler.py >> /opt/ganyu/fissler.log

