df_dim_arrival_location_sql=("""
with cte1 as (select distinct i94port as port_code,
    i94addr as state_code,
    port_name
    from staging_immigration_table sit
    join map_port mp on sit.i94port=mp.port_code and sit.i94addr = mp.port_state),
    cte2 as (select cte1.*, 
        count(*) over (partition by port_code, state_code) as check_var
        from cte1)
    select monotonically_increasing_id() as id,port_code, state_code, port_name 
    from cte2 where check_var=1
""")

df_dim_demographics_sql=("""
with cte_demographics_agg as (select * 
    from staging_demographics_table
    PIVOT (
    SUM(Count) as piv_count
    for Race in ('Hispanic or Latino' as hispanic_latino,
                    'White' as white,
                    'Black or African-American' as african_american,
                    'American Indian and Alaska Native' as native,
                    'Asian' as asian)
    ) order by City)
    select monotonically_increasing_id() as id,t1.*
    from (select distinct
    i94port as port_code,
    sdt.City as city,
    sdt.State as state,
    sdt.`State Code` as state_code,
    cast (sdt.`Median Age` as float) as median_age,
    cast (sdt.`Male Population` as long) as male_population,
    cast (sdt.`Female Population` as long) as female_population,
    cast (sdt.`Total Population` as long) as total_population,
    cast (sdt.`Number of Veterans` as long) as number_of_veterans,
    cast (sdt.`Foreign-born` as long) as foreign_born,
    cast (sdt.`Average Household Size` as double) as average_household_size,
    cast (sdt.hispanic_latino as long) as hispanic_latino_population,
    cast (sdt.white as long) as white_population,
    cast (sdt.african_american as long) as african_american_population,
    cast (sdt.native as long) as native_population,
    cast (sdt.asian as long) as asian_population
    from staging_immigration_table sit
    join map_port mp on sit.i94port=mp.port_code and sit.i94addr = mp.port_state
    join cte_demographics_agg sdt on lower(mp.port_name)=lower(sdt.City) and mp.port_state = sdt.`State Code`
    )t1
    order by id
""")

df_dim_origin_country_sql=("""
with cte1 as (select distinct i94cit as country_id from staging_immigration_table
    union 
    select distinct i94res from staging_immigration_table),
    cte2 as (
    select cast (cte1.country_id as int) as country_id, t2.country_name
    from cte1
    join map_cit_res t2
    on cte1.country_id = t2.country_id),
    cte3 as (select cte2.*,count(*) over (partition by country_id) as check_val from cte2) 
    select country_id,country_name from cte3 where check_val=1
""")

df_dim_arrival_date_sql=("""
select da.*,
    year(arrdate_conv) as year,
    month(arrdate_conv) as month,
    dayofmonth(arrdate_conv) as dayofmonth,
    dayofweek(arrdate_conv) as dayofweek,
    date_format(arrdate_conv,'E') as dayofweek_name,
    dayofyear(arrdate_conv) as dayofyear,
    weekofyear(arrdate_conv) as weekofyear,
    quarter(arrdate_conv) as quarter
    from dim_arrival_date da
""")

df_dim_junk_visa_transport_sql=("""
with cte_transport_mode as (select transport_code, 
    transport_type 
    from (select mtm.*, 
        count(transport_type) over(partition by transport_code) as check_val 
        from map_transport_mode mtm) 
        where check_val=1)
    select cast(cast(mv.visa_code as int)+100||cast(mtm.transport_code as int) as int) as id,
    cast (mv.visa_code as int) as visa_code, 
    cast (mtm.transport_code as int) as transport_code, 
    mv.visa_type, 
    mtm.transport_type 
    from map_visa mv cross join cte_transport_mode mtm
""")


df_fact_immigration_sql=("""
select monotonically_increasing_id() as id,
    dal.id as arrival_location_id,
    cast(arrdate as int) as arrival_date_id,
    djvt.id as visa_transport_id,
    doc1.country_id as country_id_citizenship,
    doc2.country_id as country_id_residence,
    dd.id as arrlocation_demographics_id,
    cast(sit.i94bir as int) as immigrant_age,
    cast(sit.biryear as int) as immigrant_birthyear,
    sit.gender as immigrant_gender,
    sit.airline
    from staging_immigration_table sit
    join dim_arrival_location dal on dal.port_code=sit.i94port
    join dim_junk_visa_transport djvt 
        on sit.i94visa=djvt.visa_code 
        and sit.i94mode=djvt.transport_code
    join dim_origin_country doc1 on doc1.country_id=sit.i94cit
    join dim_origin_country doc2 on doc2.country_id=sit.i94res
    join dim_demographics dd on dd.port_code=sit.i94port
""")
