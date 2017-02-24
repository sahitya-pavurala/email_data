drop table if exists recipient;

create external table if not exists recipient
(
    message_id string,
    sender string,
    recipient string,
    is_to integer,
    is_cc integer,
    is_bcc integer

) row format delimited fields terminated by ',' location '';