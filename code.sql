CREATE VIEW Students.v_kopach_view_module_task AS
--1)Виводити потрібну інформацію по акаунтах та групуємо по відповідним полям
with account_inf as (
SELECT
   s.date as date,
   sp.country as country,
   ac.send_interval as send_interval,
   ac.is_verified as is_verified,
   ac.is_unsubscribed as is_unsubscribed,
   COUNT (ac.id) as account_cnt
FROM `DA.account` ac
JOIN `DA.account_session` acs
ON ac.id = acs.account_id
JOIN `DA.session_params` sp
ON acs.ga_session_id = sp.ga_session_id
JOIN `DA.session` s
ON acs.ga_session_id = s.ga_session_id
GROUP BY s.date, sp.country, ac.send_interval, ac.is_verified, ac.is_unsubscribed),


--2)Вираховуємо загальну кількість створених акаунтів в розрізі країни та виводимо всі поля з попередньої таблиці для подальшої роботи
country_account_cnt as (
SELECT *, SUM (account_cnt) OVER(PARTITION BY country) as total_country_account_cnt
FROM account_inf),


--2)Виводити потрібну інформацію по листам та групуємо по відповідним полям (в підрахунку листів використовуємо DISTINCT, так як деякі листи могли відкривати два або більше разів)
message_inf as (
  SELECT
    DATE_ADD (s.date, INTERVAL ac.send_interval DAY) as date,
    sp.country as country,
    ac.send_interval as send_interval,
    ac.is_verified as is_verified,
    ac.is_unsubscribed as is_unsubscribed,
    COUNT(DISTINCT es.id_message) as sent_msg,
    COUNT (DISTINCT eo.id_message) as open_msg,
    COUNT (DISTINCT ev.id_message) as visit_msg
FROM `DA.email_sent` es
LEFT JOIN `DA.email_open` eo
ON es.id_message = eo.id_message
LEFT JOIN `DA.email_visit` ev
ON es.id_message = ev.id_message
JOIN `DA.account` ac
ON es.id_account = ac.id
JOIN `DA.account_session` acs
ON ac.id = acs.account_id
JOIN `DA.session_params` sp
ON acs.ga_session_id = sp.ga_session_id
JOIN `DA.session` s
ON acs.ga_session_id = s.ga_session_id
GROUP BY date, sp.country, ac.send_interval, ac.is_verified, ac.is_unsubscribed),


--4)Вираховуємо загальну кількість відправлених листів в розрізі країни та виводимо всі поля з попередньої таблиці для подальшої роботи
country_msg_cnt as (
SELECT *, SUM (sent_msg) OVER(PARTITION BY country)   as total_country_sent_cnt
FROM message_inf),


--5)Об'єднуємо за допомогою UNION ALL таблиці з інформацією про акаунти та листи (деяким полям надаємо значення 0, щоб зрівняти кі-сть колонок в двох таблицях)
agr_inf as (
  SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    account_cnt,
    0 as sent_msg,
    0 as open_msg,
    0 as visit_msg,
    total_country_account_cnt,
    0 as total_country_sent_cnt
FROM country_account_cnt


UNION ALL


SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    0 as account_cnt,
    sent_msg,
    open_msg,
    visit_msg,
    0 as total_country_account_cnt,
    total_country_sent_cnt
FROM country_msg_cnt),


--6)Сумуємо відповідні поля, щоб позбутися 0 в таблиці
summary_inf as (
 SELECT
   date,
   country,
   send_interval,
   is_verified,
   is_unsubscribed,
   SUM (account_cnt) as account_cnt,
   SUM(sent_msg) as sent_msg,
   SUM (open_msg) as open_msg,
   SUM (visit_msg) as visit_msg,
   SUM (total_country_account_cnt) as total_country_account_cnt,
   SUM (total_country_sent_cnt) as total_country_sent_cnt
FROM agr_inf
GROUP BY date,country, send_interval, is_verified, is_unsubscribed
),


--7)в окремому СТЕ визначаємо ранги по відповідним полям (не могли зробити це в попередньому СТЕ через групування)
rank_query as (
SELECT
   date,
   country,
   send_interval,
   is_verified,
   is_unsubscribed,
   account_cnt,
   sent_msg,
   open_msg,  
   visit_msg,
   total_country_account_cnt,
   total_country_sent_cnt,
   DENSE_RANK() OVER(ORDER BY total_country_account_cnt DESC) as rank_total_country_account_cnt,
   DENSE_RANK() OVER(ORDER BY total_country_sent_cnt DESC) as rank_total_country_sent_cnt
FROM summary_inf)




--8)виводимо кінцевий результат, застосувавши фільтрування для рангу
SELECT *
FROM rank_query
WHERE rank_total_country_account_cnt <= 10;
























