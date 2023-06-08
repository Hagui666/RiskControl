


-- 已放款 用戶訂單資料
-- RecordRaw
-- 擷取到 2023.4.1 之前放款訂單

select o.user_id, o.order_no, o.leng_order_no, o.is_olduser, o.device_money, o.lend_money, 
from_unixtime(o.add_time,'%Y-%m-%d') '生成日期',
from_unixtime(o.loan_time,'%Y-%m-%d') 放款日期,
from_unixtime(o.expire_time,'%Y-%m-%d') 到期日期,
from_unixtime(o.finish_time,'%Y-%m-%d') 結清日期,
o.status, o.is_passive_leng, o.is_leng,
from_unixtime(l.wait_time,'%Y-%m-%d') 到期還款日期,
from_unixtime(l.pay_time,'%Y-%m-%d') 還款日期
from hs_order o 
left join hs_order_loan l on o.order_no = l.order_no
where o.status in (9,10,11,12) 
and l.loan_state not in (0,4,5) 
and from_unixtime(o.loan_time) <= '2023-04-01';



-- 全部用戶(含拒絕) 風控資料
-- 部分缺失 風控標籤

SELECT 
    rc.user_id,
    rc.provider,
    rc.score,
    rc.`status`,
    rc.`rank`,
    MIN(rc.create_time) AS create_time
FROM
    hs_risk_control_order rc
WHERE
    rc.`status` = 1
GROUP BY rc.user_id
ORDER BY rc.user_id ASC;

-- 填補風控標籤缺失的 風控資料

SELECT 
    rc.user_id,
    rc.provider,
    rc.score,
    rc.`status`,
    rc.`rank`,
    MIN(rc.create_time) AS create_time
FROM
    hs_risk_control_order rc
WHERE
    rc.`status` = 1 and
    rc.`rank` is not null and
    rc.user_id in (5984,11289,34021,125020,195350,269226,280780,295953,300648,302215,303536,303969,304172,304647,304722,305027,305279,305927,306901,307580,307732,308170,311355,311976,312795,312896,312905,313445,314111,314157,314547,315456,315673,315887,316573,316990,317388,317514,319085,320677,322187,322349,323207,335094,335268,335472,335862,336065,336317,336442,336550,336605,336672,336985,337001,337639,337686,338009,338022,338092,338114,338126,338151,338383,338517,338528,338743,338984,339197,339553,339594,339606,339638,339895,339974,340097,340201,340345,340429,340625,340797,341043,341143,341236,341564,341596,341902,342230,342330,342394,343074,343253,343380,343701,343914,343940,344148,344343,345032,346154,346303,346379,347109,347349,347456,349104,349224,349697,350228,350533,351815,352092)
GROUP BY rc.user_id
ORDER BY rc.user_id ASC;



-- 已還款用戶個資
-- user realname data 

SELECT 
    c.user_id, c.birthday, c.gender
FROM
    hs_user_realname_certification c
WHERE
    c.user_id IN (SELECT 
            o.user_id
        FROM
            hs_order o
        WHERE
            status IN (8 , 9, 10, 11, 12)
                AND FROM_UNIXTIME(o.add_time, '%Y-%m-%d') < '2023-04-01');	
				
				
				
-- 已放款用戶 app list 
-- 篩選每個用戶第一張訂單生成之前的app 資料
-- 擷取到 2023.4.1 之前生成訂單

SELECT 
    a.*, FROM_UNIXTIME(o.add_time)
FROM
    hs_user_apps_log a
        INNER JOIN
    hs_order o ON o.user_id = a.user_id
WHERE
    o.id IN (SELECT 
            MIN(id)
        FROM
            hs_order
        WHERE
            status IN (8 , 9, 10, 11, 12)
        GROUP BY user_id)
        AND FROM_UNIXTIME(o.add_time, '%Y-%m-%d') > '2023-04-01'
        AND FROM_UNIXTIME(o.add_time) > a.created_time; 
		
		

-- 已放款用戶 通訊錄
-- 

SELECT 
    hs_user_contacts.user_id,
    hs_user_contacts.contacts_phone,
    hs_user_contacts.contacts_name,
    hs_user_contacts.created_time,
    FROM_UNIXTIME(o.add_time) as add_time
FROM
    hs_user_contacts
        INNER JOIN
    hs_order o ON o.user_id = hs_user_contacts.user_id
WHERE
    o.id IN (SELECT 
            MIN(id)
        FROM
            hs_order
        WHERE
            status IN (8 , 9, 10, 11, 12)
        GROUP BY user_id)
        AND FROM_UNIXTIME(o.add_time, '%Y-%m-%d') BETWEEN '2023-03-01' AND '2023-03-31'
        AND FROM_UNIXTIME(o.add_time) > hs_user_contacts.created_time;