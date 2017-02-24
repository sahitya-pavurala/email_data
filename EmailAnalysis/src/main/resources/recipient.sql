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


  drop_recipient: "DROP TABLE IF EXISTS  recipient;"
  create_recipient: "CREATE TABLE `recipient` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `message_id` varchar(250) NOT NULL DEFAULT '',
  `sender` varchar(150) DEFAULT NULL,
  `recipient` varchar(150) DEFAULT '',
  `is_to` tinyint(1) DEFAULT NULL,
  `is_cc` tinyint(1) DEFAULT NULL,
  `is_bcc` tinyint(1) DEFAULT NULL,