import pandas as pd 
import sqlalchemy

teradata_engine = sqlalchemy.create_engine('teradatasql://chnccp_db_u28074879:Metro@082020@164.61.235.21')
postgre_engine = sqlalchemy.create_engine('postgresql://metro@10.250.131.75:1526/metabase')

# maotai uv
maota_uv_sql = '''delete from chnccp_msi_z.maotai_uv'''
teradata_engine.execute(maota_uv_sql)

query_uv = '''select distinct date(a.fromat) as fromat, a.storekey||'_'||b.store_name as store_id, a.storekey as home_store_id, a.custkey as cust_no, a.cardholderkey as auth_person_id, a.event, a.paidmember as plus from mptrack.kpi a
        join store_id b
        on a.storekey = b.store_id 
	where a.event in ('mp_maotaiIndex_view', 'mp_entranceindex_tomaotai_click')
	and date(a.fromat) between '2020-08-05' and current_date -1'''
df = pd.read_sql(query_uv, postgre_engine)
df.to_sql('maotai_uv', schema = 'chnccp_msi_z', con = teradata_engine, if_exists = 'append', index = False)


# maotai succeeded namelist
maotai_namelist_sql = '''delete from chnccp_msi_z.sales_namelist'''
teradata_engine.execute(maotai_namelist_sql)
query_namelist = '''select distinct CAST(split_part(zz.buyer_id, '_', 1) as INTEGER) as home_store_id, 
        CAST(split_part(zz.buyer_id, '_', 2) as INTEGER) as cust_no, 
        CAST(split_part(zz.buyer_id, '_', 3) as INTEGER) as auth_person_id,
        zz.coupon_status, zz.created_at from 
        (select t.buyer_id, b.coupon_status, t.created_at::timestamp without time zone as created_at
        from eorder.trades t
        left join eorder.orders o
        on t.id = o.trade_id
        left join eproduct.items i
        on o.item_id = i.id
        left join eproduct.skus s 
        on o.sku_id = s.id
        left join coupon_data.couponlist b
        on o.id = b.trade_no
        where t.app_id = 'flashsale-moutai'
        and b.coupon_status is not null )zz
        '''
df1 = pd.read_sql(query_namelist, postgre_engine)
df1.to_sql('sales_namelist', schema = 'chnccp_msi_z', con = teradata_engine, if_exists = 'append', index = False)


# maotai_orders
maotai_orders_sql = '''delete from maotai_orders'''
postgre_engine.execeute(maotai_orders_sql)
query_maotai_orders = '''select o.id, t.created_at::timestamp without time zone, t.buyer_id,
        i.title as art_name, o.payment/100 as sales, o.num as qty, t.status,
        b.coupon_status, b.use_store, z.store_id, z.store_id||'_'||z.store_name as store_name, 
        (TIMESTAMP WITHOUT TIME ZONE 'epoch' + b.redeem_time * INTERVAL '1 second') as redeem_time
        from eorder.trades t
        left join eorder.orders o
        on t.id = o.trade_id
        left join eproduct.items i
        on o.item_id = i.id
        left join eproduct.skus s 
        on o.sku_id = s.id
        left join coupon_data.couponlist b
        on o.id = b.trade_no
        left join store_id z
        on z.store_name = b.use_store
        where t.app_id = 'flashsale-moutai''
'''
df2 = pd.read_sql(query_maotai_orders, postgre_engine)
df2.to_sql('maotai_orders', con = postgre_engine, if_exists = 'append', index = False)