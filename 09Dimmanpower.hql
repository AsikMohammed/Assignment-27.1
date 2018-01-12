--CreatesTable:DimManpower 
--RequiresScript:08dbo_vw_dimmanpower_stg_01.sql
set hive.auto.convert.join.noconditionaltask.size = 25000000;
set hive.auto.convert.join=false;
set hive.auto.convert.sortmerge.join=true;
set hive.tez.auto.reducer.parallelism=true;
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.cbo.enable=true;
set hive.compute.query.using.stats=true;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=100;



CREATE TABLE IF NOT EXISTS ${hivevar:DS_DATABASE}.DimManpower(
dimmanpowerkey bigint,
previous_dimmanpowerkey bigint,
businesskey string,
naturalkey decimal(15,1),
sourcesystemkey int,
datasourcekey int,
startdatekey int,
leavingdatekey int,
sitekey bigint,
departmentkey bigint,
categorykey bigint,
gradekey bigint,
gradestatuskey bigint,
contractkey bigint,
activitykey bigint,
statuskey bigint,
snropsmanagerkey  decimal(15,1),
opsmanagerkey  decimal(15,1),
linemanagerkey  decimal(15,1),
lastname string,
firstname string,
activeflag string,
shortname string,
sortname string,
timezone decimal(15, 1),
seniority string,
emailaddress string,
memo string,
pin string,
pin2 string,
badgenumber string,
payroll string,
cupid string,
siebellogon string,
hourspw string,
shiftpattern string,
tech_start_date date,
tech_end_date date,
jrn_flag string)
partitioned by (tech_datestamp date, tech_type string)
STORED AS ORC tblproperties ('orc.compress'='SNAPPY'); 

ALTER TABLE ${hivevar:DS_DATABASE}.dimmanpower
DROP IF EXISTS PARTITION (tech_datestamp='${hivevar:READPARTITION}');

DROP TABLE IF EXISTS ${hivevar:DS_DATABASE}.SOURCE_dave_pl_dimmanpower;
CREATE TABLE ${hivevar:DS_DATABASE}.SOURCE_dave_pl_dimmanpower AS
SELECT MAIN.DimManpower_Switch_PrevKey,
       ID BusinessKey,
       eWFMEmpSK NaturalKey,
       s.SystemId SourceSystemKey,
       MAIN.EWFMInstanceKey DatasourceKey,
       MAIN.StartDateKey,
       CASE
           WHEN MAIN.LeavingDateKey IS NULL
                OR MAIN.LeavingDateKey='' THEN 0
           ELSE MAIN.LeavingDateKey
       END AS LeavingDateKey,
       CASE
           WHEN SITE.DimManpowerSiteKey IS NULL
                OR SITE.DimManpowerSiteKey='' THEN 0
           ELSE SITE.DimManpowerSiteKey
       END AS SiteKey,
       CASE
           WHEN Dept.DimManpowerDeptKey IS NULL
                OR Dept.DimManpowerDeptKey='' THEN 0
           ELSE Dept.DimManpowerDeptKey
       END AS DepartmentKey,
       CASE
           WHEN CAT.DimManpowerCategoryKey IS NULL
                OR CAT.DimManpowerCategoryKey='' THEN 0
           ELSE CAT.DimManpowerCategoryKey
       END AS CategoryKey,
       CASE
           WHEN GRD.DimManpowerGradeKey IS NULL
                OR GRD.DimManpowerGradeKey='' THEN 0
           ELSE GRD.DimManpowerGradeKey
       END AS GradeKey,
       CASE
           WHEN SUB.DimManpowerGradeStatusKey IS NULL
                OR SUB.DimManpowerGradeStatusKey='' THEN 0
           ELSE SUB.DimManpowerGradeStatusKey
       END AS GradeStatusKey,
       CASE
           WHEN CON.DimManpowerContractKey IS NULL
                OR CON.DimManpowerContractKey='' THEN 0
           ELSE CON.DimManpowerContractKey
       END AS ContractKey,
       CASE
           WHEN ACT.DimManpowerActivityKey IS NULL
                OR ACT.DimManpowerActivityKey='' THEN 0
           ELSE ACT.DimManpowerActivityKey
       END AS ActivityKey,
       CASE
           WHEN STAT.DimManpowerStatusKey IS NULL
                OR STAT.DimManpowerStatusKey='' THEN 0
           ELSE STAT.DimManpowerStatusKey
       END AS StatusKey,
       CASE
           WHEN SOM.DimManpowerKey IS NULL
                OR SOM.DimManpowerKey='' THEN 0
           ELSE SOM.DimManpowerKey
       END AS SnrOpsManagerKey,
       CASE
           WHEN OM.DimManpowerKey IS NULL
                OR OM.DimManpowerKey='' THEN 0
           ELSE OM.DimManpowerKey
       END AS OpsManagerKey,
       CASE
           WHEN LM.DimManpowerKey IS NULL
                OR LM.DimManpowerKey='' THEN 0
           ELSE LM.DimManpowerKey
       END AS LineManagerKey,
       MAIN.LastName,
       MAIN.FirstName,
       MAIN.ActiveFlag,
       MAIN.ShortName,
       MAIN.SortName,
       MAIN.TimeZoneKey TimeZone,
       MAIN.Seniority,
       MAIN.EmailAddress,
       MAIN.Memo,
       MAIN.Pin,
       MAIN.Pin2,
       MAIN.BadgeNumber,
       MAIN.Payroll,
       MAIN.Cupid,
       SiebelLon SiebelLogon,
       MAIN.HoursPw,
       MAIN.ShiftPatternKey ShiftPattern,
       MAIN.DimManpowerCurrentFlag,
       MAIN.tech_type
FROM ${hivevar:DS_DATABASE}.vw_DimManpower_Stg_01 MAIN
LEFT OUTER JOIN ${hivevar:DS_DATABASE}.DimManpowercategory CAT ON UPPER(trim(MAIN.CATERY))=UPPER(trim(CAT.BkCategoryCode))
AND CAT.DatasourceId = MAIN.EWFMINSTANCEKEY
AND CAT.tech_datestamp='${hivevar:READPARTITION}'
AND CAT.tech_type='OPEN'
LEFT OUTER JOIN ${hivevar:DS_DATABASE}.DimManpowerSite SITE ON UPPER(trim(MAIN.SiteKey))=UPPER(trim(SITE.BkSiteCode))
AND SITE.DatasourceId =MAIN.EWFMINSTANCEKEY
AND SITE.tech_datestamp='${hivevar:READPARTITION}'
AND SITE.tech_type='OPEN'
LEFT OUTER JOIN ${hivevar:DS_DATABASE}.dimmanpowerdepartment DEPT ON UPPER(trim(MAIN.DeptKey))=UPPER(trim(Dept.BkDepartmentCode))
AND Dept.DatasourceId =MAIN.EWFMINSTANCEKEY
AND DEPT.tech_datestamp='${hivevar:READPARTITION}'
AND DEPT.tech_type='OPEN'
LEFT OUTER JOIN ${hivevar:DS_DATABASE}.DimManpowerGrade GRD ON UPPER(trim(MAIN.GradeKey))=UPPER(trim(GRD.BkGradeCode))
AND GRD.DatasourceId =MAIN.EWFMINSTANCEKEY
AND GRD.tech_datestamp='${hivevar:READPARTITION}'
AND GRD.tech_type='OPEN'
LEFT OUTER JOIN ${hivevar:DS_DATABASE}.DimManpowerContract CON ON UPPER(trim(MAIN.ContractKey))=UPPER(trim(CON.BkContractcode))
AND CON.DatasourceId =MAIN.EWFMINSTANCEKEY
AND CON.tech_datestamp='${hivevar:READPARTITION}'
AND CON.tech_type='OPEN'
LEFT OUTER JOIN ${hivevar:DS_DATABASE}.DimManpowerActivity ACT ON UPPER(trim(MAIN.ActivityKey))=UPPER(trim(Act.BkActivitycode))
AND Act.DatasourceId =MAIN.EWFMINSTANCEKEY
AND ACT.tech_datestamp='${hivevar:READPARTITION}'
AND ACT.tech_type='OPEN'
LEFT OUTER JOIN ${hivevar:DS_DATABASE}.DimManpowerStatus STAT ON UPPER(trim(MAIN.StatusKey))=UPPER(trim(STAT.BkStatuscode))
AND STAT.DatasourceId =MAIN.EWFMINSTANCEKEY
AND STAT.tech_datestamp='${hivevar:READPARTITION}'
AND STAT.tech_type='OPEN'
LEFT OUTER JOIN ${hivevar:DS_DATABASE}.DimManpowerGradeStatus SUB ON UPPER(trim(MAIN.SubstantiveKey))=UPPER(trim(SUB.BkGradeStatusCode))
AND SUB.DatasourceId =MAIN.EWFMINSTANCEKEY
AND SUB.tech_datestamp='${hivevar:READPARTITION}'
AND SUB.tech_type='OPEN'
INNER JOIN ${hivevar:TAMI_REF_DATABASE}.tamiref_datasource ds ON int(MAIN.EWFMInstanceKey) = int(ds.DatasourceId)
INNER JOIN ${hivevar:TAMI_REF_DATABASE}.system s ON ds.DatasourceSystemId = s.SystemId
LEFT OUTER JOIN ${hivevar:DS_DATABASE}.DimManpower LM ON MAIN.LineManagerKey = LM.NaturalKey
AND MAIN.EWFMINSTANCEKEY = LM.DatasourceKey
AND LM.tech_datestamp=DATE_SUB('${hivevar:READPARTITION}',1)
AND LM.tech_type='OPEN'
LEFT OUTER JOIN ${hivevar:DS_DATABASE}.DimManpower OM ON MAIN.OpsManagerKey = OM.NaturalKey
AND MAIN.EWFMINSTANCEKEY = OM.DatasourceKey
AND OM.tech_datestamp=DATE_SUB('${hivevar:READPARTITION}',1)
AND OM.tech_type='OPEN'
LEFT OUTER JOIN ${hivevar:DS_DATABASE}.DimManpower SOM ON MAIN.SnrOpsManagerKey = SOM.NaturalKey
AND MAIN.EWFMINSTANCEKEY = SOM.DatasourceKey
AND SOM.tech_datestamp=DATE_SUB('${hivevar:READPARTITION}',1)
AND SOM.tech_type='OPEN';


DROP TABLE IF EXISTS ${hivevar:DS_DATABASE}.DimManpower_CLOSED;


CREATE TABLE ${hivevar:DS_DATABASE}.DimManpower_CLOSED AS
SELECT TARGET.DimManpowerKey,
       TARGET.Previous_DimManpowerKey,
       TARGET.BusinessKey,
       TARGET.NaturalKey,
       TARGET.SourceSystemKey,
       TARGET.DatasourceKey,
       TARGET.StartDateKey,
       TARGET.LeavingDateKey,
       TARGET.SiteKey,
       TARGET.DepartmentKey,
       TARGET.CategoryKey,
       TARGET.GradeKey,
       TARGET.GradeStatusKey,
       TARGET.ContractKey,
       TARGET.ActivityKey,
       TARGET.StatusKey,
       TARGET.SnrOpsManagerKey,
       TARGET.OpsManagerKey,
       TARGET.LineManagerKey,
       TARGET.LastName,
       TARGET.FirstName,
       TARGET.ActiveFlag,
       TARGET.ShortName,
       TARGET.SortName,
       TARGET.TimeZone,
       TARGET.Seniority,
       TARGET.EmailAddress,
       TARGET.Memo,
       TARGET.Pin,
       TARGET.Pin2,
       TARGET.BadgeNumber,
       TARGET.Payroll,
       TARGET.Cupid,
       TARGET.SiebelLogon,
       TARGET.HoursPw,
       TARGET.ShiftPattern,
       TARGET.tech_start_date,
       '${hivevar:READPARTITION}' AS tech_end_date,
       'CLOSED' AS jrn_flag,
       '${hivevar:READPARTITION}' AS tech_datestamp,
       'CLOSED' AS tech_type
FROM
  (SELECT *
   FROM ${hivevar:DS_DATABASE}.DimManpower
   WHERE tech_datestamp = DATE_SUB('${hivevar:READPARTITION}',1)
     AND tech_type ='OPEN') TARGET
INNER JOIN ${hivevar:DS_DATABASE}.SOURCE_dave_pl_dimmanpower SOURCE ON (TARGET.NaturalKey = SOURCE.NaturalKey
                                                                        AND TARGET.DatasourceKey = SOURCE.DatasourceKey)
WHERE (trim(UPPER(SOURCE.BusinessKey)) <> trim(UPPER(TARGET.BusinessKey))
       OR SOURCE.SourceSystemKey <> TARGET.SourceSystemKey
       OR SOURCE.StartDateKey <> TARGET.StartDateKey
       OR SOURCE.LeavingDateKey <> TARGET.LeavingDateKey
       OR SOURCE.SiteKey <> TARGET.SiteKey
       OR SOURCE.DepartmentKey <> TARGET.DepartmentKey
       OR SOURCE.CategoryKey <> TARGET.CategoryKey
       OR SOURCE.GradeKey <> TARGET.GradeKey
       OR SOURCE.GradeStatusKey <> TARGET.GradeStatusKey
       OR SOURCE.ContractKey <> TARGET.ContractKey
       OR SOURCE.ActivityKey <> TARGET.ActivityKey
       OR SOURCE.StatusKey <> TARGET.StatusKey
       OR SOURCE.SnrOpsManagerKey <> TARGET.SnrOpsManagerKey
       OR SOURCE.OpsManagerKey <> TARGET.OpsManagerKey
       OR SOURCE.LineManagerKey <> TARGET.LineManagerKey
       OR trim(UPPER(SOURCE.LastName)) <> trim(UPPER(TARGET.LastName))
       OR trim(UPPER(SOURCE.FirstName)) <> trim(UPPER(TARGET.FirstName))
       OR trim(UPPER(SOURCE.ActiveFlag)) <> trim(UPPER(TARGET.ActiveFlag))
       OR trim(UPPER(SOURCE.ShortName)) <> trim(UPPER(TARGET.ShortName))
       OR trim(UPPER(SOURCE.SortName)) <> trim(UPPER(TARGET.SortName))
       OR SOURCE.TimeZone <> TARGET.TimeZone
       OR (CASE WHEN SOURCE.Seniority IS NULL
           OR SOURCE.Seniority='' THEN -9 ELSE trim(UPPER(SOURCE.Seniority)) END <> CASE WHEN TARGET.Seniority IS NULL
           OR TARGET.Seniority='' THEN -9 ELSE trim(UPPER(TARGET.Seniority)) END)
       OR (CASE WHEN SOURCE.EmailAddress IS NULL
           OR SOURCE.EmailAddress='' THEN -9 ELSE trim(UPPER(SOURCE.EmailAddress)) END <> CASE WHEN TARGET.EmailAddress IS NULL
           OR TARGET.EmailAddress='' THEN -9 ELSE trim(UPPER(TARGET.EmailAddress)) END)
       OR (CASE WHEN SOURCE.Pin IS NULL
           OR SOURCE.Pin='' THEN -9 ELSE trim(UPPER(SOURCE.Pin)) END <> CASE WHEN TARGET.Pin IS NULL
           OR TARGET.Pin='' THEN -9 ELSE trim(UPPER(TARGET.Pin)) END)
       OR (CASE WHEN SOURCE.Pin2 IS NULL
           OR SOURCE.Pin2='' THEN -9 ELSE trim(UPPER(SOURCE.Pin2)) END <> CASE WHEN TARGET.Pin2 IS NULL
           OR TARGET.Pin2='' THEN -9 ELSE trim(UPPER(TARGET.Pin2)) END)
       OR (CASE WHEN SOURCE.BadgeNumber IS NULL
           OR SOURCE.BadgeNumber='' THEN -9 ELSE trim(UPPER(SOURCE.BadgeNumber)) END <> CASE WHEN TARGET.BadgeNumber IS NULL
           OR TARGET.BadgeNumber='' THEN -9 ELSE trim(UPPER(TARGET.BadgeNumber)) END)
       OR (CASE WHEN SOURCE.Payroll IS NULL
           OR SOURCE.Payroll='' THEN -9 ELSE trim(UPPER(SOURCE.Payroll)) END <> CASE WHEN TARGET.Payroll IS NULL
           OR TARGET.Payroll='' THEN -9 ELSE trim(UPPER(TARGET.Payroll)) END)
       OR (CASE WHEN SOURCE.Cupid IS NULL
           OR SOURCE.Cupid='' THEN -9 ELSE trim(UPPER(SOURCE.Cupid)) END <> CASE WHEN TARGET.Cupid IS NULL
           OR TARGET.Cupid='' THEN -9 ELSE trim(UPPER(TARGET.Cupid)) END)
       OR (CASE WHEN SOURCE.SiebelLogon IS NULL
           OR SOURCE.SiebelLogon='' THEN -9 ELSE trim(UPPER(SOURCE.SiebelLogon)) END <> CASE WHEN TARGET.SiebelLogon IS NULL
           OR TARGET.SiebelLogon='' THEN -9 ELSE trim(UPPER(TARGET.SiebelLogon)) END)
       OR (CASE WHEN SOURCE.HoursPw IS NULL
           OR SOURCE.HoursPw='' THEN -9 ELSE trim(UPPER(SOURCE.HoursPw)) END <> CASE WHEN TARGET.HoursPw IS NULL
           OR TARGET.HoursPw='' THEN -9 ELSE trim(UPPER(TARGET.HoursPw)) END)
       OR (CASE WHEN SOURCE.ShiftPattern IS NULL
           OR SOURCE.ShiftPattern='' THEN -9 ELSE trim(UPPER(SOURCE.ShiftPattern)) END <> CASE WHEN TARGET.ShiftPattern IS NULL
           OR TARGET.ShiftPattern='' THEN -9 ELSE trim(UPPER(TARGET.ShiftPattern)) END)
       OR (CASE WHEN SOURCE.Memo IS NULL
           OR SOURCE.Memo='' THEN -9 ELSE trim(UPPER(SOURCE.Memo)) END <> CASE WHEN TARGET.Memo IS NULL
           OR TARGET.Memo='' THEN -9 ELSE trim(UPPER(TARGET.Memo)) END)
       OR SOURCE.tech_type = 'CLOSED');


DROP TABLE IF EXISTS ${hivevar:DS_DATABASE}.DimManpower_TARGET_OPEN;


CREATE TABLE ${hivevar:DS_DATABASE}.DimManpower_TARGET_OPEN AS
SELECT TARGET.DimManpowerKey,
       TARGET.Previous_DimManpowerKey,
       TARGET.BusinessKey,
       TARGET.NaturalKey,
       cast(TARGET.SourceSystemKey as string) as SourceSystemKey,
       TARGET.DatasourceKey,
       TARGET.StartDateKey,
       TARGET.LeavingDateKey,
       TARGET.SiteKey,
       TARGET.DepartmentKey,
       TARGET.CategoryKey,
       TARGET.GradeKey,
       TARGET.GradeStatusKey,
       TARGET.ContractKey,
       TARGET.ActivityKey,
       TARGET.StatusKey,
       cast(TARGET.SnrOpsManagerKey as bigint) as SnrOpsManagerKey,
       cast(TARGET.OpsManagerKey as bigint) as OpsManagerKey,
       cast(TARGET.LineManagerKey as bigint) as LineManagerKey,
       TARGET.LastName,
       TARGET.FirstName,
       TARGET.ActiveFlag,
       TARGET.ShortName,
       TARGET.SortName,
       TARGET.TimeZone,
       TARGET.Seniority,
       TARGET.EmailAddress,
       TARGET.Memo,
       TARGET.Pin,
       TARGET.Pin2,
       TARGET.BadgeNumber,
       TARGET.Payroll,
       TARGET.Cupid,
       TARGET.SiebelLogon,
       TARGET.HoursPw,
       TARGET.ShiftPattern,
       cast(TARGET.tech_start_date as string) as tech_start_date,
       cast(TARGET.tech_end_date as string) as tech_end_date,
       TARGET.jrn_flag,
       '${hivevar:READPARTITION}' AS tech_datestamp,
       TARGET.tech_type
FROM
  (SELECT *
   FROM ${hivevar:DS_DATABASE}.DimManpower
   WHERE tech_datestamp = DATE_SUB('${hivevar:READPARTITION}',1)
     AND tech_type ='OPEN') TARGET
LEFT OUTER JOIN ${hivevar:DS_DATABASE}.DimManpower_CLOSED closed_temp ON (TARGET.NaturalKey = closed_temp.NaturalKey
                                                                          AND TARGET.DatasourceKey = closed_temp.DatasourceKey)
WHERE closed_temp.NaturalKey IS NULL
  AND closed_temp.DatasourceKey IS NULL;


DROP TABLE IF EXISTS max_surkey_update;


CREATE
TEMPORARY TABLE max_surkey_update AS
SELECT CASE
           WHEN max_i_key IS NULL THEN 0
           ELSE max_i_key
       END max_key
FROM
  (SELECT max(DimManpowerKey) max_i_key
   FROM ${hivevar:DS_DATABASE}.DimManpower
   WHERE tech_datestamp = DATE_SUB('${hivevar:READPARTITION}',1)) max_surkey;


DROP TABLE IF EXISTS ${hivevar:DS_DATABASE}.DimManpower_UPDATE;

CREATE TABLE ${hivevar:DS_DATABASE}.DimManpower_UPDATE AS
SELECT max_surkey_update.max_key + rank() over (
                                                ORDER BY rand()) AS DimManpowerKey,
       TARGET.DimManpowerKey AS Previous_DimManpowerKey,
       SOURCE.BusinessKey,
       SOURCE.NaturalKey,
       SOURCE.SourceSystemKey,
       SOURCE.DatasourceKey,
       SOURCE.StartDateKey,
       SOURCE.LeavingDateKey,
       SOURCE.SiteKey,
       SOURCE.DepartmentKey,
       SOURCE.CategoryKey,
       SOURCE.GradeKey,
       SOURCE.GradeStatusKey,
       SOURCE.ContractKey,
       SOURCE.ActivityKey,
       SOURCE.StatusKey,
       SOURCE.SnrOpsManagerKey,
       SOURCE.OpsManagerKey,
       SOURCE.LineManagerKey,
       SOURCE.LastName,
       SOURCE.FirstName,
       SOURCE.ActiveFlag,
       SOURCE.ShortName,
       SOURCE.SortName,
       SOURCE.TimeZone,
       SOURCE.Seniority,
       SOURCE.EmailAddress,
       SOURCE.Memo,
       SOURCE.Pin,
       SOURCE.Pin2,
       SOURCE.BadgeNumber,
       SOURCE.Payroll,
       SOURCE.Cupid,
       SOURCE.SiebelLogon,
       SOURCE.HoursPw,
       SOURCE.ShiftPattern,
       '${hivevar:READPARTITION}' AS tech_start_date,
       '9999-12-31' AS tech_end_date,
       'UPDATE' AS jrn_flag,
       '${hivevar:READPARTITION}' AS tech_datestamp,
       'OPEN' AS tech_type
FROM
  (SELECT *
   FROM ${hivevar:DS_DATABASE}.DimManpower
   WHERE tech_datestamp = DATE_SUB('${hivevar:READPARTITION}',1)
     AND tech_type ='OPEN') TARGET
INNER JOIN ${hivevar:DS_DATABASE}.Source_dave_pl_DimManpower SOURCE ON (TARGET.NaturalKey = SOURCE.NaturalKey
                                                                        AND TARGET.DatasourceKey = SOURCE.DatasourceKey)
INNER JOIN max_surkey_update ON 1=1
WHERE SOURCE.tech_type ='OPEN'
	AND (trim(UPPER(SOURCE.BusinessKey)) <> trim(UPPER(TARGET.BusinessKey))
       OR SOURCE.SourceSystemKey <> TARGET.SourceSystemKey
       OR SOURCE.StartDateKey <> TARGET.StartDateKey
       OR SOURCE.LeavingDateKey <> TARGET.LeavingDateKey
       OR SOURCE.SiteKey <> TARGET.SiteKey
       OR SOURCE.DepartmentKey <> TARGET.DepartmentKey
       OR SOURCE.CategoryKey <> TARGET.CategoryKey
       OR SOURCE.GradeKey <> TARGET.GradeKey
       OR SOURCE.GradeStatusKey <> TARGET.GradeStatusKey
       OR SOURCE.ContractKey <> TARGET.ContractKey
       OR SOURCE.ActivityKey <> TARGET.ActivityKey
       OR SOURCE.StatusKey <> TARGET.StatusKey
       OR SOURCE.SnrOpsManagerKey <> TARGET.SnrOpsManagerKey
       OR SOURCE.OpsManagerKey <> TARGET.OpsManagerKey
       OR SOURCE.LineManagerKey <> TARGET.LineManagerKey
       OR trim(UPPER(SOURCE.LastName)) <> trim(UPPER(TARGET.LastName))
       OR trim(UPPER(SOURCE.FirstName)) <> trim(UPPER(TARGET.FirstName))
       OR trim(UPPER(SOURCE.ActiveFlag)) <> trim(UPPER(TARGET.ActiveFlag))
       OR trim(UPPER(SOURCE.ShortName)) <> trim(UPPER(TARGET.ShortName))
       OR trim(UPPER(SOURCE.SortName)) <> trim(UPPER(TARGET.SortName))
       OR SOURCE.TimeZone <> TARGET.TimeZone
       OR (CASE WHEN SOURCE.Seniority IS NULL
           OR SOURCE.Seniority='' THEN -9 ELSE trim(UPPER(SOURCE.Seniority)) END <> CASE WHEN TARGET.Seniority IS NULL
           OR TARGET.Seniority='' THEN -9 ELSE trim(UPPER(TARGET.Seniority)) END)
       OR (CASE WHEN SOURCE.EmailAddress IS NULL
           OR SOURCE.EmailAddress='' THEN -9 ELSE trim(UPPER(SOURCE.EmailAddress)) END <> CASE WHEN TARGET.EmailAddress IS NULL
           OR TARGET.EmailAddress='' THEN -9 ELSE trim(UPPER(TARGET.EmailAddress)) END)
       OR (CASE WHEN SOURCE.Pin IS NULL
           OR SOURCE.Pin='' THEN -9 ELSE trim(UPPER(SOURCE.Pin)) END <> CASE WHEN TARGET.Pin IS NULL
           OR TARGET.Pin='' THEN -9 ELSE trim(UPPER(TARGET.Pin)) END)
       OR (CASE WHEN SOURCE.Pin2 IS NULL
           OR SOURCE.Pin2='' THEN -9 ELSE trim(UPPER(SOURCE.Pin2)) END <> CASE WHEN TARGET.Pin2 IS NULL
           OR TARGET.Pin2='' THEN -9 ELSE trim(UPPER(TARGET.Pin2)) END)
       OR (CASE WHEN SOURCE.BadgeNumber IS NULL
           OR SOURCE.BadgeNumber='' THEN -9 ELSE trim(UPPER(SOURCE.BadgeNumber)) END <> CASE WHEN TARGET.BadgeNumber IS NULL
           OR TARGET.BadgeNumber='' THEN -9 ELSE trim(UPPER(TARGET.BadgeNumber)) END)
       OR (CASE WHEN SOURCE.Payroll IS NULL
           OR SOURCE.Payroll='' THEN -9 ELSE trim(UPPER(SOURCE.Payroll)) END <> CASE WHEN TARGET.Payroll IS NULL
           OR TARGET.Payroll='' THEN -9 ELSE trim(UPPER(TARGET.Payroll)) END)
       OR (CASE WHEN SOURCE.Cupid IS NULL
           OR SOURCE.Cupid='' THEN -9 ELSE trim(UPPER(SOURCE.Cupid)) END <> CASE WHEN TARGET.Cupid IS NULL
           OR TARGET.Cupid='' THEN -9 ELSE trim(UPPER(TARGET.Cupid)) END)
       OR (CASE WHEN SOURCE.SiebelLogon IS NULL
           OR SOURCE.SiebelLogon='' THEN -9 ELSE trim(UPPER(SOURCE.SiebelLogon)) END <> CASE WHEN TARGET.SiebelLogon IS NULL
           OR TARGET.SiebelLogon='' THEN -9 ELSE trim(UPPER(TARGET.SiebelLogon)) END)
       OR (CASE WHEN SOURCE.HoursPw IS NULL
           OR SOURCE.HoursPw='' THEN -9 ELSE trim(UPPER(SOURCE.HoursPw)) END <> CASE WHEN TARGET.HoursPw IS NULL
           OR TARGET.HoursPw='' THEN -9 ELSE trim(UPPER(TARGET.HoursPw)) END)
       OR (CASE WHEN SOURCE.ShiftPattern IS NULL
           OR SOURCE.ShiftPattern='' THEN -9 ELSE trim(UPPER(SOURCE.ShiftPattern)) END <> CASE WHEN TARGET.ShiftPattern IS NULL
           OR TARGET.ShiftPattern='' THEN -9 ELSE trim(UPPER(TARGET.ShiftPattern)) END)
       OR (CASE WHEN SOURCE.Memo IS NULL
           OR SOURCE.Memo='' THEN -9 ELSE trim(UPPER(SOURCE.Memo)) END <> CASE WHEN TARGET.Memo IS NULL
           OR TARGET.Memo='' THEN -9 ELSE trim(UPPER(TARGET.Memo)) END)
       OR SOURCE.tech_type = 'CLOSED');


DROP TABLE IF EXISTS max_surkey_insert;


CREATE
TEMPORARY TABLE max_surkey_insert AS
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
     (SELECT max(DimManpowerKey) max_i_key
      FROM ${hivevar:DS_DATABASE}.DimManpower_UPDATE) a
   INNER JOIN
     (SELECT max_key max_u_key
      FROM max_surkey_update) b ON 1=1) max_surkey;


DROP TABLE IF EXISTS ${hivevar:DS_DATABASE}.DimManpower_INSERT;


CREATE TABLE ${hivevar:DS_DATABASE}.DimManpower_INSERT AS
SELECT max_surkey_insert.max_key + rank() over (
                                                ORDER BY rand()) AS DimManpowerKey,
       SOURCE.DimManpower_Switch_PrevKey AS Previous_DimManpowerKey,
       SOURCE.BusinessKey,
       SOURCE.NaturalKey,
       SOURCE.SourceSystemKey,
       SOURCE.DatasourceKey,
       SOURCE.StartDateKey,
       SOURCE.LeavingDateKey,
       SOURCE.SiteKey,
       SOURCE.DepartmentKey,
       SOURCE.CategoryKey,
       SOURCE.GradeKey,
       SOURCE.GradeStatusKey,
       SOURCE.ContractKey,
       SOURCE.ActivityKey,
       SOURCE.StatusKey,
       SOURCE.SnrOpsManagerKey,
       SOURCE.OpsManagerKey,
       SOURCE.LineManagerKey,
       SOURCE.LastName,
       SOURCE.FirstName,
       SOURCE.ActiveFlag,
       SOURCE.ShortName,
       SOURCE.SortName,
       SOURCE.TimeZone,
       SOURCE.Seniority,
       SOURCE.EmailAddress,
       SOURCE.Memo,
       SOURCE.Pin,
       SOURCE.Pin2,
       SOURCE.BadgeNumber,
       SOURCE.Payroll,
       SOURCE.Cupid,
       SOURCE.SiebelLogon,
       SOURCE.HoursPw,
       SOURCE.ShiftPattern,
       '${hivevar:READPARTITION}' AS tech_start_date,
       '9999-12-31' AS tech_end_date,
       'INSERT' AS jrn_flag,
       '${hivevar:READPARTITION}' AS tech_datestamp,
       'OPEN' AS tech_type
FROM
  (SELECT *
   FROM ${hivevar:DS_DATABASE}.DimManpower
   WHERE tech_datestamp = DATE_SUB('${hivevar:READPARTITION}',1)
     AND tech_type ='OPEN') TARGET
RIGHT OUTER JOIN ${hivevar:DS_DATABASE}.Source_dave_pl_DimManpower SOURCE ON(TARGET.NaturalKey = SOURCE.NaturalKey
                                                                             AND TARGET.DatasourceKey = SOURCE.DatasourceKey)
INNER JOIN max_surkey_insert ON 1=1
WHERE (TARGET.NaturalKey IS NULL
       AND TARGET.DatasourceKey IS NULL);

---------------------------------------------------------------------------------------------
-----Secondary update statement required to ensure that on 1st run, management details are
----populated and also to deal with new starter management structures when both the staff
-----and manager are identified as new in the same run.
---------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS ${hivevar:DS_DATABASE}.DimManpower_OPEN;


CREATE TABLE ${hivevar:DS_DATABASE}.DimManpower_OPEN AS
SELECT *
FROM ${hivevar:DS_DATABASE}.DimManpower_TARGET_OPEN
UNION ALL
SELECT *
FROM ${hivevar:DS_DATABASE}.DimManpower_UPDATE
UNION ALL
SELECT *
FROM ${hivevar:DS_DATABASE}.DimManpower_INSERT;


DROP TABLE IF EXISTS ${hivevar:DS_DATABASE}.DimManpower_updated;


CREATE TABLE IF NOT EXISTS ${hivevar:DS_DATABASE}.DimManpower_updated AS
SELECT DISTINCT TARGET.DimManpowerKey,
                TARGET.Previous_DimManpowerKey,
                TARGET.BusinessKey,
                TARGET.NaturalKey,
                TARGET.SourceSystemKey,
                TARGET.DatasourceKey,
                TARGET.StartDateKey,
                TARGET.LeavingDateKey,
                TARGET.SiteKey,
                TARGET.DepartmentKey,
                TARGET.CategoryKey,
                TARGET.GradeKey,
                TARGET.GradeStatusKey,
                TARGET.ContractKey,
                TARGET.ActivityKey,
                TARGET.StatusKey,
                CASE
                    WHEN updated_subq.DimManpowerKey IS NOT NULL THEN updated_subq.SnrOpsManagerKey
                    ELSE TARGET.SnrOpsManagerKey
                END SnrOpsManagerKey,
                CASE
                    WHEN updated_subq.DimManpowerKey IS NOT NULL THEN updated_subq.OpsManagerKey
                    ELSE TARGET.OpsManagerKey
                END OpsManagerKey,
                CASE
                    WHEN updated_subq.DimManpowerKey IS NOT NULL THEN updated_subq.LineManagerKey
                    ELSE TARGET.LineManagerKey
                END LineManagerKey,
                TARGET.LastName,
                TARGET.FirstName,
                TARGET.ActiveFlag,
                TARGET.ShortName,
                TARGET.SortName,
                TARGET.TimeZone,
                TARGET.Seniority,
                TARGET.EmailAddress,
                TARGET.Memo,
                TARGET.Pin,
                TARGET.Pin2,
                TARGET.BadgeNumber,
                TARGET.Payroll,
                TARGET.Cupid,
                TARGET.SiebelLogon,
                TARGET.HoursPw,
                TARGET.ShiftPattern,
                TARGET.tech_start_date,
                TARGET.tech_end_date,
                TARGET.jrn_flag,
                TARGET.tech_datestamp,
                TARGET.tech_type
FROM ${hivevar:DS_DATABASE}.DimManpower_OPEN TARGET
LEFT OUTER JOIN
  (SELECT DISTINCT EMP.dimmanpowerkey dimmanpowerkey,
                   EMP.Naturalkey NaturalKey,
                   EMP.BusinessKey BusinessKey,
                   CASE
                       WHEN SOM.DimManpowerKey IS NULL
                            OR SOM.DimManpowerKey='' THEN 0
                       ELSE SOM.DimManpowerKey
                   END AS SnrOpsManagerKey,
                          CASE
                              WHEN OM.DimManpowerKey IS NULL
                                   OR OM.DimManpowerKey='' THEN 0
                              ELSE OM.DimManpowerKey
                          END AS OpsManagerKey,
                                 CASE
                                     WHEN LM.DimManpowerKey IS NULL
                                          OR LM.DimManpowerKey='' THEN 0
                                     ELSE LM.DimManpowerKey
                                 END AS LineManagerKey
   FROM ${hivevar:DS_DATABASE}.vw_DimManpower_Stg_01 MAIN
   LEFT JOIN ${hivevar:DS_DATABASE}.DimManpower_OPEN EMP ON MAIN.EWFMEMPSK = EMP.NaturalKey
   AND MAIN.EWFMINSTANCEKEY = EMP.DatasourceKey
   LEFT JOIN ${hivevar:DS_DATABASE}.DimManpower_OPEN LM ON MAIN.LineManagerKey = LM.NaturalKey
   AND MAIN.EWFMINSTANCEKEY = LM.DatasourceKey
   LEFT JOIN ${hivevar:DS_DATABASE}.DimManpower_OPEN OM ON MAIN.OpsManagerKey = OM.NaturalKey
   AND MAIN.EWFMINSTANCEKEY = OM.DatasourceKey
   LEFT JOIN ${hivevar:DS_DATABASE}.DimManpower_OPEN SOM ON MAIN.SnrOpsManagerKey = SOM.NaturalKey
   AND MAIN.EWFMINSTANCEKEY = SOM.DatasourceKey
   WHERE (EMP.LineManagerKey != CASE WHEN LM.DimManpowerKey IS NULL
          OR LM.DimManpowerKey='' THEN 0 ELSE LM.DimManpowerKey END
          OR EMP.OpsManagerKey != CASE WHEN OM.DimManpowerKey IS NULL
          OR OM.DimManpowerKey='' THEN 0 ELSE OM.DimManpowerKey END
          OR EMP.SnrOpsManagerKey != CASE WHEN SOM.DimManpowerKey IS NULL
          OR SOM.DimManpowerKey='' THEN 0 ELSE SOM.DimManpowerKey END)) updated_subq ON TARGET.DimManpowerKey = updated_subq.DimManpowerKey
AND TARGET.NaturalKey = updated_subq.NaturalKey
AND trim(upper(TARGET.BusinessKey)) = trim(upper(updated_subq.BusinessKey));


INSERT INTO TABLE ${hivevar:DS_DATABASE}.DimManpower PARTITION(tech_datestamp, tech_type)
SELECT *
FROM ${hivevar:DS_DATABASE}.DimManpower_CLOSED;


INSERT INTO TABLE ${hivevar:DS_DATABASE}.DimManpower PARTITION(tech_datestamp, tech_type)
SELECT *
FROM ${hivevar:DS_DATABASE}.DimManpower_updated;

-----Insert merge victim records records into ManpowerMergeVictim
------------------------------similar to closed----

CREATE TABLE IF NOT EXISTS ${hivevar:DS_DATABASE}.ManpowerMergeVictim 
(dimmanpowerkey bigint, 
tech_start_date date, 
tech_end_date date, 
jrn_flag string)
partitioned BY (tech_datestamp date, tech_type string) 
STORED AS ORC tblproperties ('orc.compress'='SNAPPY');


ALTER TABLE ${hivevar:DS_DATABASE}.ManpowerMergeVictim
DROP IF EXISTS PARTITION (tech_datestamp='${hivevar:READPARTITION}');
INSERT INTO TABLE ${hivevar:DS_DATABASE}.ManpowerMergeVictim
PARTITION(tech_datestamp, tech_type)
SELECT
  DimManpowerKey,
  tech_start_date,
  tech_end_date,
  jrn_flag,
  tech_datestamp,
  tech_type
FROM ${hivevar:DS_DATABASE}.DimManpower_CLOSED;

