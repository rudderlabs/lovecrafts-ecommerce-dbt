
select optin, label, count(distinct dwh_visitor_id) as users from 
(select
                    case when b.dwh_visitor_id is not null then 1 else 0 end as label, a.dwh_visitor_id  ,
                    first_value(c.optin) over(partition by a.dwh_visitor_id order by c.before_date desc rows unbounded preceding) as optin
                from
                    (
                        select *
                        from event_stream_customer_features
                        where
                            "timestamp" between '2022-06-01' and '2022-06-02'
                            and days_since_last_seen <= 180
                    ) a
                left join
                    (
                        select dwh_visitor_id, "timestamp"
                        from event_stream_customer_features
                        where
                            "timestamp" between '2022-12-01' and '2022-12-02'
                            and days_since_last_seen > 180
                    ) b
                    on a.dwh_visitor_id = b.dwh_visitor_id  left join 
                    (select distinct dwh_visitor_id, before_date, optin 
                    from rudderstack_external.rfm_champions_by_month__subscriber_status a inner join  dbt_production.user_mapping b on a.user_id = b.user_id) c on 
                    a.dwh_visitor_id = c.dwh_visitor_id
                where
                    a."timestamp">=c.before_date) a group by 1,2

/*
Results:
                    Optin: 0 - 38742, 1 - 7104 - 15.4%
                    Optout: 0 - 6872, 1 - 2085 - 23.2% 
                    # All optin champion users, active in prev 180 days

                    */