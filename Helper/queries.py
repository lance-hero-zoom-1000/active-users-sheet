from datetime import datetime

supply_query = """
select 
    * 
from dineout_supply_master
"""

curfew_tag = """
select 
    restaurant_id as r_id, 
    tag_name 
from tags_restaurant 
where 
    lower(tag_name) like '%curfew%'
"""

bookings_query = """
select 
    a.*, 
    b.city_name as city,
    (case when b_id in (select distinct booking_id from order_master
    where paid_status=5 and obj_type in ("deal")) then "deal" 
    when b_id in (select distinct booking_id from order_master
    where paid_status=5 and obj_type in ("event")) then "event" 
    else "booking" end) as booking_type,
    (case when b_id in (select bm.b_id from booking_master as bm
    inner join ir_booking_map as ibm on ibm.b_id = bm.b_id
    where auto_book_flag<>1 and ibm.vendor_to_do_status=1
    and bm.b_id not in (select booking_id from order_master
    where paid_status=5 and obj_type in ("deal","event"))) then 1 else 0 end) as inresto_actioned,
    (case when b_id in (select b_id from booking_master
    where auto_book_flag<>1 and (b_id not in (select booking_id from order_master
    where paid_status=5 and obj_type in ("deal","event")))
    and b_id not in (select bm.b_id from booking_master as bm
    inner join ir_booking_map as ibm on ibm.b_id = bm.b_id
    where auto_book_flag<>1 and ibm.vendor_to_do_status=1
    and bm.b_id not in (select booking_id from order_master
    where paid_status=5 and obj_type in ("deal","event")))) then 1 else 0 end) as outbound_actioned
from booking_master as a
    left join restaurant_master as b on a.restaurant_id=b.r_id
    left join offer_master as c on a.offer_id=c.offer_id
where
    date(a.creation_dt) between date('{sdate}') and date('{edate}')
"""

do_pay_query = """
select 
    omf.*,
    rm.city_name as city_name
from order_master_flat omf
left join restaurant_master rm
on omf.restaurant_id = rm.r_id
where
    paid_status=5
    and obj_type in ('restaurant', 'deal', 'booking', 'event', 'coupon')
    and date(created_on) between date('{sdate}') and date('{edate}')
"""

new_diners_bookings = """
select 
    bm.diner_id, 
    min(date(bm.creation_dt)) as first_booking_date,
    min(bm.b_id) as first_id,
    sub.restaurant_id as first_restaurant,
    rm.city_name as first_city
from booking_master bm
left join booking_master sub
on bm.b_id = sub.b_id
left join restaurant_master rm
on sub.restaurant_id = rm.r_id
group by 1
UNION
select 
    omf.diner_id, 
    min(date(omf.created_on)) as first_booking_date,
    min(omf.id) as first_id,
    sub.restaurant_id as first_restaurant,
    rm.city_name as first_city
from order_master_flat omf
left join order_master_flat sub
on omf.id = sub.id
left join restaurant_master rm
on sub.restaurant_id = rm.r_id
where
    omf.paid_status=5
    and omf.obj_type in ('restaurant')
group by 1
"""

new_diners_transactions = """
select 
    omf.diner_id, 
    min(date(omf.created_on)) as first_transaction_date,
    min(omf.id) as first_id,
    sub.restaurant_id as first_restaurant,
    rm.city_name as first_city
from order_master_flat omf
left join order_master_flat sub
on omf.id = sub.id
left join restaurant_master rm
on sub.restaurant_id = rm.r_id
where
    omf.paid_status=5
    and omf.obj_type in ('restaurant', 'deal', 'booking', 'event', 'coupon')
group by 1
"""

affiliate_bookings = """
select
    b_id,
    (
        case when affiliate_id=1063 then 'Google Booking' 
        else 'Affiliate Booking'
        end
    ) as affiliate_booking_src
from affiliate_booking
where b_id in {booking_ids}
"""

sign_up_query = """
select 
    d_id as diner_id, 
    d_dt
from diner
"""

mau_data_month = """
select *
from mau_data_historic_month mdhm 
WHERE 
	date(`month`) between date('{sdate}') and date('{edate}')
union
select *
from mau_data_current_month mdcm 
"""

mau_data_week = """
-- query for previous 4 weeks + current week as week to date
select *
from mau_data_historic_week mdhw 
WHERE 
	date(`week_start_date`) between date('{sdate}') and date('{edate}')
union
select *
from mau_data_current_week mdcw 
"""

mau_data_day = """
select *
from mau_data_daily
where 
	`date` between date('{sdate}') and date('{edate}')
"""
