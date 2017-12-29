set hive.auto.convert.join.noconditionaltask.size = 25000000;
set hive.auto.convert.join=true;
set hive.auto.convert.sortmerge.join=true;
set hive.tez.auto.reducer.parallelism=true;
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.cbo.enable=true;
set hive.compute.query.using.stats=true;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=100;



CREATE TABLE IF NOT EXISTS ${hivevar:DS_DATABASE}.inboundtelephony_bgs_vdn_hiera (
davesk bigint,
bkhierask int,
switchsk int,
vdn string,
callsource string,
callsubtype string,
calltype string,
callgroup string,
subpl string,
pl string,
businessunit string,
transfer_flag string,
validfrom string,
validto string,
customer_facing_flag string,
scorecard_flag string,
bgs_vdn_hiera_spareattribute01 string,
bgs_vdn_hiera_spareattribute02 string,
bgs_vdn_hiera_spareattribute03 string,
bgs_vdn_hiera_spareattribute04 string,
bgs_vdn_hiera_spareattribute05 string,
bgs_vdn_hiera_spareattribute06 string,
bgs_vdn_hiera_spareattribute07 string,
bgs_vdn_hiera_spareattribute08 string,
bgs_vdn_hiera_spareattribute09 string,
bgs_vdn_hiera_spareattribute10 string,
tech_start_date date,
tech_end_date date,
jrn_flag string)
partitioned by (tech_datestamp date, tech_type string)
STORED AS ORC tblproperties ('orc.compress'='SNAPPY'); 

ALTER TABLE ${hivevar:DS_DATABASE}.inboundtelephony_bgs_vdn_hiera
DROP IF EXISTS PARTITION(tech_datestamp='${hivevar:READPARTITION}');


----CREATING Temporary source table------
DROP TABLE IF EXISTS ${hivevar:DS_DATABASE}.source_bgs_vdn_hiera;


CREATE TEMPORARY TABLE ${hivevar:DS_DATABASE}.source_bgs_vdn_hiera AS
SELECT hierask AS bkhierask,
       switchid AS switchsk,
       dnis AS vdn,
       NVL(callsource.callsourcename,'Unknown') AS callsource,
       NVL(subType.callsubtypedesc,'Unknown') AS callsubtype,
       NVL(calltype.calltypedesc,'Unknown') AS calltype,
       NVL(callgroup.callgroupname,'Unknown') AS callgroup,
       NVL(callgroup.callgroupname,'Unknown') AS subpl,
       NVL(callgroup.callgroupname,'Unknown') AS pl,
       'BGS' AS businessunit,
       CASE
           WHEN trim(UPPER(callsource.callsourcedesc)) = 'ICM' THEN 'N'
           ELSE 'Y'
       END transfer_flag,
       validfrom AS validfrom,
       validuntil AS validto,
       diallednumber AS bgs_vdn_hiera_spareattribute01
FROM ${hivevar:TAMI_REF_DATABASE}.tamiref_mi_vdnmap vdn
INNER JOIN ${hivevar:TAMI_REF_DATABASE}.tamiref_mi_tblswitch switch ON switch.id = vdn.switchid
INNER JOIN ${hivevar:TAMI_REF_DATABASE}.tamiref_mi_tblsystem SYSTEM ON SYSTEM.id = switch.systemid
LEFT OUTER JOIN ${hivevar:TAMI_REF_DATABASE}.tamiref_mi_tblcallsubtype subType ON vdn.callsubtypeid = subType.id
LEFT OUTER JOIN ${hivevar:TAMI_REF_DATABASE}.tamiref_mi_tblcalltype calltype ON CAST(vdn.calltypeannid AS INT) = calltype.id
LEFT OUTER JOIN ${hivevar:TAMI_REF_DATABASE}.tamiref_mi_tblcallgroup callgroup ON calltype.callgroupid = CallGroup.callgroupid
LEFT OUTER JOIN ${hivevar:TAMI_REF_DATABASE}.tamiref_MI_tblCallSource callsource ON vdn.callsourceid = callsource.id
WHERE trim(UPPER(SYSTEM.systemname)) = 'ASPECT';


------CLOSED TABLE---
DROP TABLE IF EXISTS ${hivevar:DS_DATABASE}.bgs_vdn_hiera_closed;
CREATE TEMPORARY TABLE ${hivevar:DS_DATABASE}.bgs_vdn_hiera_closed (
davesk bigint,
bkhierask int,
switchsk int,
vdn string,
callsource string,
callsubtype string,
calltype string,
callgroup string,
subpl string,
pl string,
businessunit string,
transfer_flag string,
validfrom string,
validto string,
customer_facing_flag string,
scorecard_flag string,
bgs_vdn_hiera_spareattribute01 string,
bgs_vdn_hiera_spareattribute02 string,
bgs_vdn_hiera_spareattribute03 string,
bgs_vdn_hiera_spareattribute04 string,
bgs_vdn_hiera_spareattribute05 string,
bgs_vdn_hiera_spareattribute06 string,
bgs_vdn_hiera_spareattribute07 string,
bgs_vdn_hiera_spareattribute08 string,
bgs_vdn_hiera_spareattribute09 string,
bgs_vdn_hiera_spareattribute10 string,
tech_start_date date,
tech_end_date date,
jrn_flag string,
tech_datestamp date, 
tech_type string);


---YESTERDAY's, Unchanged Records
DROP TABLE IF EXISTS ${hivevar:DS_DATABASE}.bgs_vdn_hiera_target_open;


CREATE TEMPORARY TABLE ${hivevar:DS_DATABASE}.bgs_vdn_hiera_target_open LIKE ${hivevar:DS_DATABASE}.bgs_vdn_hiera_closed;

 -----Updated New Matching records

DROP TABLE IF EXISTS ${hivevar:DS_DATABASE}.bgs_vdn_hiera_update;


CREATE TEMPORARY TABLE ${hivevar:DS_DATABASE}.bgs_vdn_hiera_update LIKE ${hivevar:DS_DATABASE}.bgs_vdn_hiera_closed;

 -------Inserted NEW Records

DROP TABLE IF EXISTS ${hivevar:DS_DATABASE}.bgs_vdn_hiera_insert;


CREATE TEMPORARY TABLE ${hivevar:DS_DATABASE}.bgs_vdn_hiera_insert LIKE ${hivevar:DS_DATABASE}.bgs_vdn_hiera_closed;


DROP TABLE IF EXISTS ${hivevar:DS_DATABASE}.max_surkey_update;


CREATE
TEMPORARY TABLE ${hivevar:DS_DATABASE}.max_surkey_update AS
SELECT CASE
           WHEN max_i_key IS NULL THEN 0
           ELSE max_i_key
       END max_key
FROM
  (SELECT max(davesk) max_i_key
   FROM ${hivevar:DS_DATABASE}.inboundtelephony_bgs_vdn_hiera
   WHERE tech_datestamp = Date_SUB('${hivevar:READPARTITION}', 1)) max_surkey;




FROM (
(SELECT *
   FROM ${hivevar:DS_DATABASE}.inboundtelephony_bgs_vdn_hiera
   WHERE tech_datestamp = DATE_SUB('${hivevar:READPARTITION}', 1)
     AND tech_type ='OPEN') target
INNER JOIN ${hivevar:DS_DATABASE}.source_bgs_vdn_hiera SOURCE ON (target.bkhierask = SOURCE.bkhierask)
INNER JOIN ${hivevar:DS_DATABASE}.max_surkey_update ON 1=1 )
INSERT INTO TABLE ${hivevar:DS_DATABASE}.bgs_vdn_hiera_closed
SELECT target.davesk,
       target.bkhierask,
       target.switchsk,
       target.vdn,
       target.callsource,
       target.callsubtype,
       target.calltype,
       target.callgroup,
       target.subpl,
       target.pl,
       target.businessunit,
       target.transfer_flag,
       target.validfrom,
       target.validto,
       target.customer_facing_flag,
       target.scorecard_flag,
       target.bgs_vdn_hiera_spareattribute01,
       target.bgs_vdn_hiera_spareattribute02,
       target.bgs_vdn_hiera_spareattribute03,
       target.bgs_vdn_hiera_spareattribute04,
       target.bgs_vdn_hiera_spareattribute05,
       target.bgs_vdn_hiera_spareattribute06,
       target.bgs_vdn_hiera_spareattribute07,
       target.bgs_vdn_hiera_spareattribute08,
       target.bgs_vdn_hiera_spareattribute09,
       target.bgs_vdn_hiera_spareattribute10,
       target.tech_start_date,
       '${hivevar:READPARTITION}' AS tech_end_date,
       'CLOSED' AS jrn_flag,
       '${hivevar:READPARTITION}' AS tech_datestamp,
       'CLOSED' AS tech_type
WHERE (target.switchsk <> SOURCE.switchsk
       OR trim(upper(target.vdn)) <> trim(upper(SOURCE.vdn))
       OR trim(upper(target.callsource)) <> trim(upper(SOURCE.callsource))
       OR trim(upper(target.callsubtype)) <> trim(upper(SOURCE.callsubtype))
       OR trim(upper(target.calltype)) <> trim(upper(SOURCE.calltype))
       OR trim(upper(target.callgroup)) <> trim(upper(SOURCE.callgroup))
       OR trim(upper(target.subpl)) <> trim(upper(SOURCE.subpl))
       OR trim(upper(target.pl)) <> trim(upper(SOURCE.pl))
       OR trim(upper(target.businessunit)) <> trim(upper(SOURCE.businessunit))
       OR trim(upper(target.transfer_flag)) <> trim(upper(SOURCE.transfer_flag))
       OR trim(upper(target.bgs_vdn_hiera_spareattribute01)) <> trim(upper(SOURCE.bgs_vdn_hiera_spareattribute01)))
INSERT INTO TABLE ${hivevar:DS_DATABASE}.bgs_vdn_hiera_update
SELECT max_surkey_update.max_key + rank() over (
                                                ORDER BY rand()),
       target.bkhierask,
       target.switchsk,
       target.vdn,
       source.callsource,
       source.callsubtype,
       source.calltype,
       source.callgroup,
       source.subpl,
       source.pl,
       source.businessunit,
       source.transfer_flag,
       target.validfrom,
       source.validto,
       target.customer_facing_flag,
       target.scorecard_flag,
       target.bgs_vdn_hiera_spareattribute01,
       target.bgs_vdn_hiera_spareattribute02,
       target.bgs_vdn_hiera_spareattribute03,
       target.bgs_vdn_hiera_spareattribute04,
       target.bgs_vdn_hiera_spareattribute05,
       target.bgs_vdn_hiera_spareattribute06,
       target.bgs_vdn_hiera_spareattribute07,
       target.bgs_vdn_hiera_spareattribute08,
       target.bgs_vdn_hiera_spareattribute09,
       target.bgs_vdn_hiera_spareattribute10,
       '${hivevar:READPARTITION}' AS tech_start_date,
       '9999-12-31' AS tech_end_date,
       'UPDATE' AS jrn_flag,
       '${hivevar:READPARTITION}' AS tech_datestamp,
       'OPEN' AS tech_type
WHERE (target.switchsk <> SOURCE.switchsk
       OR trim(upper(target.vdn)) <> trim(upper(SOURCE.vdn))
       OR trim(upper(target.callsource)) <> trim(upper(SOURCE.callsource))
       OR trim(upper(target.callsubtype)) <> trim(upper(SOURCE.callsubtype))
       OR trim(upper(target.calltype)) <> trim(upper(SOURCE.calltype))
       OR trim(upper(target.callgroup)) <> trim(upper(SOURCE.callgroup))
       OR trim(upper(target.subpl)) <> trim(upper(SOURCE.subpl))
       OR trim(upper(target.pl)) <> trim(upper(SOURCE.pl))
       OR trim(upper(target.businessunit)) <> trim(upper(SOURCE.businessunit))
       OR trim(upper(target.transfer_flag)) <> trim(upper(SOURCE.transfer_flag))
       OR trim(upper(target.bgs_vdn_hiera_spareattribute01)) <> trim(upper(SOURCE.bgs_vdn_hiera_spareattribute01)));

-----UNCHANGED RECORDS FROM YESTERDAY

INSERT INTO TABLE ${hivevar:DS_DATABASE}.bgs_vdn_hiera_target_open
SELECT target.davesk,
       target.bkhierask,
       target.switchsk,
       target.vdn,
       target.callsource,
       target.callsubtype,
       target.calltype,
       target.callgroup,
       target.subpl,
       target.pl,
       target.businessunit,
       target.transfer_flag,
       target.validfrom,
       target.validto,
       target.customer_facing_flag,
       target.scorecard_flag,
       target.bgs_vdn_hiera_spareattribute01,
       target.bgs_vdn_hiera_spareattribute02,
       target.bgs_vdn_hiera_spareattribute03,
       target.bgs_vdn_hiera_spareattribute04,
       target.bgs_vdn_hiera_spareattribute05,
       target.bgs_vdn_hiera_spareattribute06,
       target.bgs_vdn_hiera_spareattribute07,
       target.bgs_vdn_hiera_spareattribute08,
       target.bgs_vdn_hiera_spareattribute09,
       target.bgs_vdn_hiera_spareattribute10,
       target.tech_start_date,
       target.tech_end_date,
       target.jrn_flag,
       '${hivevar:READPARTITION}' AS tech_datestamp,
       target.tech_type
FROM
  (SELECT *
   FROM ${hivevar:DS_DATABASE}.inboundtelephony_bgs_vdn_hiera
   WHERE tech_datestamp = DATE_SUB('${hivevar:READPARTITION}', 1)
     AND tech_type ='OPEN') target
LEFT OUTER JOIN ${hivevar:DS_DATABASE}.bgs_vdn_hiera_closed closed ON (target.bkhierask = closed.bkhierask)
WHERE closed.bkhierask IS NULL;





-------------------------------------------INSERT new records from source----------------------------------------
DROP TABLE IF EXISTS ${hivevar:DS_DATABASE}.max_surkey_insert;


CREATE
TEMPORARY TABLE ${hivevar:DS_DATABASE}.max_surkey_insert AS
SELECT CASE
           WHEN max_i_key IS NULL
                AND max_u_key IS NULL THEN 0
           WHEN max_i_key IS NULL THEN max_u_key
           ELSE max_i_key
       END max_key
FROM
  (SELECT max_i_key,
          max_u_key
   FROM
     (SELECT max(davesk) max_i_key
      FROM ${hivevar:DS_DATABASE}.bgs_vdn_hiera_update) a
   INNER JOIN
     (SELECT max_key max_u_key
      FROM ${hivevar:DS_DATABASE}.max_surkey_update) b ON 1=1) max_surkey;




INSERT INTO TABLE ${hivevar:DS_DATABASE}.bgs_vdn_hiera_insert
SELECT max_surkey_insert.max_key + rank() over (
                                                ORDER BY rand()),
       source.bkhierask,
       source.switchsk,
       source.vdn,
       source.callsource,
       source.callsubtype,
       source.calltype,
       source.callgroup,
       source.subpl,
       source.pl,
       source.businessunit,
       source.transfer_flag,
       source.validfrom,
       source.validto,
       NULL AS customer_facing_flag,
       NULL AS scorecard_flag,
       source.bgs_vdn_hiera_spareattribute01,
       NULL AS asbgs_vdn_hiera_spareattribute02,
       NULL AS asbgs_vdn_hiera_spareattribute03,
       NULL AS asbgs_vdn_hiera_spareattribute04,
       NULL AS asbgs_vdn_hiera_spareattribute05,
       NULL AS asbgs_vdn_hiera_spareattribute06,
       NULL AS asbgs_vdn_hiera_spareattribute07,
       NULL AS asbgs_vdn_hiera_spareattribute08,
       NULL AS asbgs_vdn_hiera_spareattribute09,
       NULL AS asbgs_vdn_hiera_spareattribute10,
       '${hivevar:READPARTITION}' AS tech_start_date,
       '9999-12-31' AS tech_end_date,
       'INSERT' AS jrn_flag,
       '${hivevar:READPARTITION}' AS tech_datestamp,
       'OPEN' AS tech_type
FROM
  (SELECT *
   FROM ${hivevar:DS_DATABASE}.inboundtelephony_bgs_vdn_hiera
   WHERE tech_datestamp = DATE_SUB('${hivevar:READPARTITION}', 1)
     AND tech_type ='OPEN') target
RIGHT OUTER JOIN ${hivevar:DS_DATABASE}.source_bgs_vdn_hiera SOURCE ON (target.bkhierask = SOURCE.bkhierask)
INNER JOIN ${hivevar:DS_DATABASE}.max_surkey_insert ON 1=1
WHERE target.bkhierask IS NULL;
--------------------------INSERTING INTO target TABLE

INSERT INTO TABLE ${hivevar:DS_DATABASE}.inboundtelephony_bgs_vdn_hiera Partition(tech_datestamp, tech_type)
SELECT *
FROM ${hivevar:DS_DATABASE}.bgs_vdn_hiera_closed;


INSERT INTO TABLE ${hivevar:DS_DATABASE}.inboundtelephony_bgs_vdn_hiera Partition(tech_datestamp, tech_type)
SELECT *
FROM ${hivevar:DS_DATABASE}.bgs_vdn_hiera_target_open;


INSERT INTO TABLE ${hivevar:DS_DATABASE}.inboundtelephony_bgs_vdn_hiera Partition(tech_datestamp, tech_type)
SELECT *
FROM ${hivevar:DS_DATABASE}.bgs_vdn_hiera_update;


INSERT INTO TABLE ${hivevar:DS_DATABASE}.inboundtelephony_bgs_vdn_hiera Partition(tech_datestamp, tech_type)
SELECT *
FROM ${hivevar:DS_DATABASE}.bgs_vdn_hiera_insert;


---DROPPING ALL INTERMEDIATE TABLES-----
DROP TABLE IF EXISTS ${hivevar:DS_DATABASE}.source_bgs_vdn_hiera;


DROP TABLE IF EXISTS ${hivevar:DS_DATABASE}.bgs_vdn_hiera_closed;


DROP TABLE IF EXISTS ${hivevar:DS_DATABASE}.bgs_vdn_hiera_target_open;


DROP TABLE IF EXISTS ${hivevar:DS_DATABASE}.bgs_vdn_hiera_update;


DROP TABLE IF EXISTS ${hivevar:DS_DATABASE}.bgs_vdn_hiera_insert;