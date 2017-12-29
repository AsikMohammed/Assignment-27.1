CREATE TABLE IF NOT EXISTS ${hivevar:DS_DATABASE}.d_queue_group(
davesk int,
resource_id int,
open_date string,
closed_date string,
created_date string,
site string,
queue_group_key string,
switch_queue string,
queue string,
call_type string,
call_group string,
sub_p_and_l string,
p_and_l string,
business_unit string,
scorecard_flag string,
transfer_flag string,
customer_facing_flag string,
jrn_flag string,
tech_start_date string,
tech_end_date string
)partitioned by (tech_datestamp  string, tech_type string)
STORED AS ORC tblproperties ('orc.compress'='SNAPPY');

DROP TABLE IF EXISTS ${hivevar:DS_DATABASE}.max_surkey_res_id;

CREATE TABLE ${hivevar:DS_DATABASE}.max_surkey_res_id stored AS orc tblproperties ('orc.compress'='SNAPPY') AS
SELECT CASE
           WHEN max_id IS NULL THEN 0
           ELSE max_id
       END max_key
FROM
  (SELECT max(davesk) max_id
   FROM ${hivevar:DS_DATABASE}.d_queue_group
   WHERE tech_datestamp = date_sub('${hivevar:READPARTITION}',1)) max_res_id;


DROP TABLE IF EXISTS ${hivevar:DS_DATABASE}.d_queue_group_merge;

CREATE TABLE ${hivevar:DS_DATABASE}.d_queue_group_merge(
davesk int,
resource_id int,
open_date string,
closed_date string,
created_date string,
site string,
queue_group_key string,
switch_queue string,
queue string,
call_type string,
call_group string,
sub_p_and_l string,
p_and_l string,
business_unit string,
scorecard_flag string,
transfer_flag string,
customer_facing_flag string,
target_davesk int,
target_resource_id int,
target_open_date string,
target_closed_date string,
target_created_date string,
target_site string,
target_queue_group_key string,
target_switch_queue string,
target_queue string,
target_call_type string,
target_call_group string,
target_sub_p_and_l string,
target_p_and_l string,
target_business_unit string,
target_scorecard_flag string,
target_transfer_flag string,
target_customer_facing_flag string,
target_jrn_flag string,
target_tech_start_date string)
STORED AS ORC tblproperties ('orc.compress'='SNAPPY');

ALTER TABLE ${hivevar:DS_DATABASE}.d_queue_group

DROP IF EXISTS partition(tech_datestamp='${hivevar:READPARTITION}');


INSERT INTO TABLE ${hivevar:DS_DATABASE}.d_queue_group_merge
SELECT sur.max_key + rank() over (
                                  ORDER BY rand()) AS davesk,
       source.resource_id AS resource_id,
       source.open_date AS open_date,
       source.closed_date AS closed_date,
       source.created_date AS created_date,
       source.site AS site,
       source.queue_group_key AS queue_group_key,
       source.switch_queue AS switch_queue,
       source.queue AS queue,
       source.call_type AS call_type,
       source.call_group AS call_group,
       source.sub_p_and_l AS sub_p_and_l,
       source.p_and_l AS p_and_l,
       source.business_unit AS business_unit,
       source.scorecard_flag AS scorecard_flag,
       source.transfer_flag AS transfer_flag,
       source.customer_facing_flag AS customer_facing_flag,
       target.davesk AS target_davesk,
       target.resource_id AS target_resource_id,
       target.open_date AS target_open_date,
       target.closed_date AS target_closed_date,
       target.created_date AS target_created_date,
       target.site AS target_site,
       target.queue_group_key AS target_queue_group_key,
       target.switch_queue AS target_switch_queue,
       target.queue AS target_queue,
       target.call_type AS target_call_type,
       target.call_group AS target_call_group,
       target.sub_p_and_l AS target_sub_p_and_l,
       target.p_and_l AS target_p_and_l,
       target.business_unit AS target_business_unit,
       target.scorecard_flag AS target_scorecard_flag,
       target.transfer_flag AS target_transfer_flag,
       target.customer_facing_flag AS target_customer_facing_flag,
       target.jrn_flag AS target_jrn_flag,
       target.tech_start_date AS target_tech_start_date
FROM
  (SELECT *
   FROM ${hivevar:DS_DATABASE}.d_queue_group
   WHERE tech_datestamp = date_sub('${hivevar:READPARTITION}',1)
     AND tech_type='OPEN') target
FULL OUTER JOIN
  (SELECT *
   FROM ${hivevar:PROD_DATABASE}.genesys_gim_custom_owner_d_queue_group
   WHERE tech_datestamp = '${hivevar:READPARTITION}'
     AND tech_type='OPEN') SOURCE ON target.queue_group_key = SOURCE.queue_group_key
INNER JOIN ${hivevar:DS_DATABASE}.max_surkey_res_id sur ON 1=1;

--updating timestamp for not matched and old records
INSERT INTO TABLE ${hivevar:DS_DATABASE}.d_queue_group partition(tech_datestamp, tech_type)
SELECT source.target_davesk AS davesk,
       source.target_resource_id AS resource_id,
       source.target_open_date AS open_date,
       source.target_closed_date AS closed_date,
       source.target_created_date AS created_date,
       source.target_site AS site,
       source.target_queue_group_key AS queue_group_key,
       source.target_switch_queue AS switch_queue,
       source.target_queue AS queue,
       source.target_call_type AS call_type,
       source.target_call_group AS call_group,
       source.target_sub_p_and_l AS sub_p_and_l,
       source.target_p_and_l AS p_and_l,
       source.target_business_unit AS business_unit,
       source.target_scorecard_flag AS scorecard_flag,
       source.target_transfer_flag AS transfer_flag,
       source.target_customer_facing_flag AS customer_facing_flag,
       source.target_jrn_flag AS jrn_flag,
       source.target_tech_start_date AS tech_start_date,
       '9999-12-31',
       '${hivevar:READPARTITION}',
       'OPEN'
FROM ${hivevar:DS_DATABASE}.d_queue_group_merge SOURCE
WHERE SOURCE.queue_group_key IS NULL;

--closing matched and changed records
INSERT INTO TABLE ${hivevar:DS_DATABASE}.d_queue_group partition(tech_datestamp, tech_type)
SELECT source.target_davesk AS davesk,
       source.target_resource_id AS resource_id,
       source.target_open_date AS open_date,
       source.target_closed_date AS closed_date,
       source.target_created_date AS created_date,
       source.target_site AS site,
       source.target_queue_group_key AS queue_group_key,
       source.target_switch_queue AS switch_queue,
       source.target_queue AS queue,
       source.target_call_type AS call_type,
       source.target_call_group AS call_group,
       source.target_sub_p_and_l AS sub_p_and_l,
       source.target_p_and_l AS p_and_l,
       source.target_business_unit AS business_unit,
       source.target_scorecard_flag AS scorecard_flag,
       source.target_transfer_flag AS transfer_flag,
       source.target_customer_facing_flag AS customer_facing_flag,
       'CLOSED',
       source.target_tech_start_date AS target_tech_start_date,
       '${hivevar:READPARTITION}',
       '${hivevar:READPARTITION}',
       'CLOSED'
FROM ${hivevar:DS_DATABASE}.d_queue_group_merge SOURCE
WHERE SOURCE.target_queue_group_key IS NOT NULL
  AND SOURCE.queue_group_key IS NOT NULL
  AND (nvl(upper(SOURCE.resource_id),'') <> nvl(upper(SOURCE.target_resource_id),'')
       OR nvl(upper(SOURCE.open_date),'') <> nvl(upper(SOURCE.target_open_date),'')
       OR nvl(upper(SOURCE.closed_date),'') <> nvl(upper(SOURCE.target_closed_date),'')
       OR nvl(upper(SOURCE.created_date),'') <> nvl(upper(SOURCE.target_created_date),'')
       OR nvl(upper(SOURCE.site),'') <> nvl(upper(SOURCE.target_site),'')
       OR nvl(upper(SOURCE.switch_queue),'') <> nvl(upper(SOURCE.target_switch_queue),'')
       OR nvl(upper(SOURCE.queue),'') <> nvl(upper(SOURCE.target_queue),'')
       OR nvl(upper(SOURCE.call_type),'') <> nvl(upper(SOURCE.target_call_type),'')
       OR nvl(upper(SOURCE.call_group),'') <> nvl(upper(SOURCE.target_call_group),'')
       OR nvl(upper(SOURCE.sub_p_and_l),'') <> nvl(upper(SOURCE.target_sub_p_and_l),'')
       OR nvl(upper(SOURCE.p_and_l),'') <> nvl(upper(SOURCE.target_p_and_l),'')
       OR nvl(upper(SOURCE.business_unit),'') <> nvl(upper(SOURCE.target_business_unit),'')
       OR nvl(upper(SOURCE.scorecard_flag),'') <> nvl(upper(SOURCE.target_scorecard_flag),'')
       OR nvl(upper(SOURCE.transfer_flag),'') <> nvl(upper(SOURCE.target_transfer_flag),'')
       OR nvl(upper(SOURCE.customer_facing_flag),'') <> nvl(upper(SOURCE.target_customer_facing_flag),''));

-- insert matched records and changed
INSERT INTO TABLE ${hivevar:DS_DATABASE}.d_queue_group partition(tech_datestamp, tech_type)
SELECT source.davesk AS davesk,
       source.resource_id AS resource_id,
       source.open_date AS open_date,
       source.closed_date AS closed_date,
       source.created_date AS created_date,
       source.site AS site,
       source.queue_group_key AS queue_group_key,
       source.switch_queue AS switch_queue,
       source.queue AS queue,
       source.call_type AS call_type,
       source.call_group AS call_group,
       source.sub_p_and_l AS sub_p_and_l,
       source.p_and_l AS p_and_l,
       source.business_unit AS business_unit,
       source.scorecard_flag AS scorecard_flag,
       source.transfer_flag AS transfer_flag,
       source.customer_facing_flag AS customer_facing_flag,
       'UPDATE',
       '${hivevar:READPARTITION}',
       '9999-12-31',
       '${hivevar:READPARTITION}',
       'OPEN'
FROM ${hivevar:DS_DATABASE}.d_queue_group_merge SOURCE
WHERE SOURCE.target_queue_group_key IS NOT NULL
  AND SOURCE.queue_group_key IS NOT NULL
  AND (nvl(upper(SOURCE.resource_id),'') <> nvl(upper(SOURCE.target_resource_id),'')
       OR nvl(upper(SOURCE.open_date),'') <> nvl(upper(SOURCE.target_open_date),'')
       OR nvl(upper(SOURCE.closed_date),'') <> nvl(upper(SOURCE.target_closed_date),'')
       OR nvl(upper(SOURCE.created_date),'') <> nvl(upper(SOURCE.target_created_date),'')
       OR nvl(upper(SOURCE.site),'') <> nvl(upper(SOURCE.target_site),'')
       OR nvl(upper(SOURCE.switch_queue),'') <> nvl(upper(SOURCE.target_switch_queue),'')
       OR nvl(upper(SOURCE.queue),'') <> nvl(upper(SOURCE.target_queue),'')
       OR nvl(upper(SOURCE.call_type),'') <> nvl(upper(SOURCE.target_call_type),'')
       OR nvl(upper(SOURCE.call_group),'') <> nvl(upper(SOURCE.target_call_group),'')
       OR nvl(upper(SOURCE.sub_p_and_l),'') <> nvl(upper(SOURCE.target_sub_p_and_l),'')
       OR nvl(upper(SOURCE.p_and_l),'') <> nvl(upper(SOURCE.target_p_and_l),'')
       OR nvl(upper(SOURCE.business_unit),'') <> nvl(upper(SOURCE.target_business_unit),'')
       OR nvl(upper(SOURCE.scorecard_flag),'') <> nvl(upper(SOURCE.target_scorecard_flag),'')
       OR nvl(upper(SOURCE.transfer_flag),'') <> nvl(upper(SOURCE.target_transfer_flag),'')
       OR nvl(upper(SOURCE.customer_facing_flag),'') <> nvl(upper(SOURCE.target_customer_facing_flag),''));


-- update timestamp for matched records and not changed
INSERT INTO TABLE ${hivevar:DS_DATABASE}.d_queue_group partition(tech_datestamp, tech_type)
SELECT source.target_davesk AS davesk,
       source.target_resource_id AS resource_id,
       source.target_open_date AS open_date,
       source.target_closed_date AS closed_date,
       source.target_created_date AS created_date,
       source.target_site AS site,
       source.target_queue_group_key AS queue_group_key,
       source.target_switch_queue AS switch_queue,
       source.target_queue AS queue,
       source.target_call_type AS call_type,
       source.target_call_group AS call_group,
       source.target_sub_p_and_l AS sub_p_and_l,
       source.target_p_and_l AS p_and_l,
       source.target_business_unit AS business_unit,
       source.target_scorecard_flag AS scorecard_flag,
       source.target_transfer_flag AS transfer_flag,
       source.target_customer_facing_flag AS customer_facing_flag,
       source.target_jrn_flag AS jrn_flag,
       source.target_tech_start_date AS tech_start_date,
       '9999-12-31',
       '${hivevar:READPARTITION}',
       'OPEN'
FROM ${hivevar:DS_DATABASE}.d_queue_group_merge SOURCE
WHERE SOURCE.target_queue_group_key IS NOT NULL
  AND SOURCE.queue_group_key IS NOT NULL
  AND (nvl(upper(SOURCE.resource_id),'') <> nvl(upper(SOURCE.target_resource_id),'')
       OR nvl(upper(SOURCE.open_date),'') <> nvl(upper(SOURCE.target_open_date),'')
       OR nvl(upper(SOURCE.closed_date),'') <> nvl(upper(SOURCE.target_closed_date),'')
       OR nvl(upper(SOURCE.created_date),'') <> nvl(upper(SOURCE.target_created_date),'')
       OR nvl(upper(SOURCE.site),'') <> nvl(upper(SOURCE.target_site),'')
       OR nvl(upper(SOURCE.switch_queue),'') <> nvl(upper(SOURCE.target_switch_queue),'')
       OR nvl(upper(SOURCE.queue),'') <> nvl(upper(SOURCE.target_queue),'')
       OR nvl(upper(SOURCE.call_type),'') <> nvl(upper(SOURCE.target_call_type),'')
       OR nvl(upper(SOURCE.call_group),'') <> nvl(upper(SOURCE.target_call_group),'')
       OR nvl(upper(SOURCE.sub_p_and_l),'') <> nvl(upper(SOURCE.target_sub_p_and_l),'')
       OR nvl(upper(SOURCE.p_and_l),'') <> nvl(upper(SOURCE.target_p_and_l),'')
       OR nvl(upper(SOURCE.business_unit),'') <> nvl(upper(SOURCE.target_business_unit),'')
       OR nvl(upper(SOURCE.scorecard_flag),'') <> nvl(upper(SOURCE.target_scorecard_flag),'')
       OR nvl(upper(SOURCE.transfer_flag),'') <> nvl(upper(SOURCE.target_transfer_flag),'')
       OR nvl(upper(SOURCE.customer_facing_flag),'') <> nvl(upper(SOURCE.target_customer_facing_flag),'')) = FALSE;



-- insert not matched and new
INSERT INTO TABLE ${hivevar:DS_DATABASE}.d_queue_group partition(tech_datestamp, tech_type)
SELECT source.davesk AS davesk,
       source.resource_id AS resource_id,
       source.open_date AS open_date,
       source.closed_date AS closed_date,
       source.created_date AS created_date,
       source.site AS site,
       source.queue_group_key AS queue_group_key,
       source.switch_queue AS switch_queue,
       source.queue AS queue,
       source.call_type AS call_type,
       source.call_group AS call_group,
       source.sub_p_and_l AS sub_p_and_l,
       source.p_and_l AS p_and_l,
       source.business_unit AS business_unit,
       source.scorecard_flag AS scorecard_flag,
       source.transfer_flag AS transfer_flag,
       source.customer_facing_flag AS customer_facing_flag,
       'INSERT',
       '${hivevar:READPARTITION}',
       '9999-12-31',
       '${hivevar:READPARTITION}',
       'OPEN'
FROM ${hivevar:DS_DATABASE}.d_queue_group_merge SOURCE
WHERE SOURCE.target_queue_group_key IS NULL;

