drop table if exists email;

create external table if not exists email
(
    message_id string,
    sender string,
    subject string,
    email_date bigint,
    label string,
    sub_md5 string

) row format delimited fields terminated by ',' location '';