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


-------------------------------------table created for ewfm_bgr_emp----------------------------------------------

DROP TABLE IF EXISTS ${hivevar:DS_DATABASE}.ewfm_bgr_emp;
CREATE TABLE IF NOT EXISTS ${hivevar:DS_DATABASE}.ewfm_bgr_emp
(jrn_date                string,
jrn_flag                string,
tech_closure_flag       string, 
tech_start_date         string,
tech_end_date           string,
emp_sk                  string,
short_name              string,
sort_name               string,
id                      string,
last_name               string,
first_name              string,
eff_hire_nom_date       string,
term_nom_date           string,
active_flag             string,
time_zone_sk            string,
seniority               string,
email_adr               string,
memo                    string,
upd_tcs_login_sk        string,
upd_ts                  string,
im_usr_name             string,
tech_datestamp          string, 
tech_type               string, 
tech_num                string)
STORED AS ORC tblproperties ("orc.compress"="SNAPPY","orc.compress.size"="16384");


INSERT INTO TABLE ${hivevar:DS_DATABASE}.ewfm_bgr_emp
SELECT jrn_date,
       jrn_flag,
       tech_closure_flag,
       tech_start_date,
       tech_end_date,
       emp_sk,
       short_name,
       sort_name,
       id,
       last_name,
       first_name,
       eff_hire_nom_date,
       term_nom_date,
       active_flag,
       time_zone_sk,
       seniority,
       email_adr,
       memo,
       upd_tcs_login_sk,
       upd_ts,
       im_usr_name,
       tech_datestamp,
       tech_type,
       tech_num
FROM
  (SELECT jrn_date,
          jrn_flag,
          tech_closure_flag,
          tech_start_date,
          tech_end_date,
          emp_sk,
          short_name,
          sort_name,
          id,
          last_name,
          first_name,
          eff_hire_nom_date,
          term_nom_date,
          active_flag,
          time_zone_sk,
          seniority,
          email_adr,
          memo,
          upd_tcs_login_sk,
          upd_ts,
          im_usr_name,
          tech_datestamp,
          tech_type,
          tech_num,
          row_number () over (partition BY emp_sk
                              ORDER BY jrn_date DESC) rnk
   FROM ${hivevar:PROD_DATABASE}.ewfm_bgr_emp
   WHERE tech_datestamp = '${hivevar:READPARTITION}'
     AND tech_type= 'OPEN'
     AND tech_num= '1') sub_q
WHERE sub_q.rnk = 1;




-------------------------------------table created for ewfm_bgsm_emp----------------------------------------------

DROP TABLE IF EXISTS ${hivevar:DS_DATABASE}.ewfm_bgsm_emp;
CREATE TABLE IF NOT EXISTS ${hivevar:DS_DATABASE}.ewfm_bgsm_emp
(jrn_date                string,
jrn_flag                string,
tech_closure_flag       string, 
tech_start_date         string,
tech_end_date           string,
emp_sk                  string,
short_name              string,
sort_name               string,
id                      string,
last_name               string,
first_name              string,
eff_hire_nom_date       string,
term_nom_date           string,
active_flag             string,
time_zone_sk            string,
seniority               string,
email_adr               string,
memo                    string,
upd_tcs_login_sk        string,
upd_ts                  string,
im_usr_name             string,
tech_datestamp          string, 
tech_type               string, 
tech_num                string)
STORED AS ORC tblproperties ("orc.compress"="SNAPPY","orc.compress.size"="16384");


INSERT INTO TABLE ${hivevar:DS_DATABASE}.ewfm_bgsm_emp
SELECT jrn_date,
       jrn_flag,
       tech_closure_flag,
       tech_start_date,
       tech_end_date,
       emp_sk,
       short_name,
       sort_name,
       id,
       last_name,
       first_name,
       eff_hire_nom_date,
       term_nom_date,
       active_flag,
       time_zone_sk,
       seniority,
       email_adr,
       memo,
       upd_tcs_login_sk,
       upd_ts,
       im_usr_name,
       tech_datestamp,
       tech_type,
       tech_num
FROM
  (SELECT jrn_date,
          jrn_flag,
          tech_closure_flag,
          tech_start_date,
          tech_end_date,
          emp_sk,
          short_name,
          sort_name,
          id,
          last_name,
          first_name,
          eff_hire_nom_date,
          term_nom_date,
          active_flag,
          time_zone_sk,
          seniority,
          email_adr,
          memo,
          upd_tcs_login_sk,
          upd_ts,
          im_usr_name,
          tech_datestamp,
          tech_type,
          tech_num,
          row_number () over (partition BY emp_sk
                              ORDER BY jrn_date DESC) rnk
   FROM ${hivevar:PROD_DATABASE}.ewfm_bgsm_emp
   WHERE tech_datestamp = '${hivevar:READPARTITION}'
     AND tech_type= 'OPEN'
     AND tech_num= '1') sub_q
WHERE sub_q.rnk = 1;


 
 

 -------------------------------------table created for ewfm_bgs_emp----------------------------------------------

DROP TABLE IF EXISTS ${hivevar:DS_DATABASE}.ewfm_bgs_emp;
CREATE TABLE IF NOT EXISTS ${hivevar:DS_DATABASE}.ewfm_bgs_emp
(jrn_date                string,
jrn_flag                string,
tech_closure_flag       string, 
tech_start_date         string,
tech_end_date           string,
emp_sk                  string,
short_name              string,
sort_name               string,
id                      string,
last_name               string,
first_name              string,
eff_hire_nom_date       string,
term_nom_date           string,
active_flag             string,
time_zone_sk            string,
seniority               string,
email_adr               string,
memo                    string,
upd_tcs_login_sk        string,
upd_ts                  string,
im_usr_name             string,
tech_datestamp          string, 
tech_type               string, 
tech_num                string)
STORED AS ORC tblproperties ("orc.compress"="SNAPPY","orc.compress.size"="16384");


INSERT INTO TABLE ${hivevar:DS_DATABASE}.ewfm_bgs_emp
SELECT jrn_date,
       jrn_flag,
       tech_closure_flag,
       tech_start_date,
       tech_end_date,
       emp_sk,
       short_name,
       sort_name,
       id,
       last_name,
       first_name,
       eff_hire_nom_date,
       term_nom_date,
       active_flag,
       time_zone_sk,
       seniority,
       email_adr,
       memo,
       upd_tcs_login_sk,
       upd_ts,
       im_usr_name,
       tech_datestamp,
       tech_type,
       tech_num
FROM
  (SELECT jrn_date,
          jrn_flag,
          tech_closure_flag,
          tech_start_date,
          tech_end_date,
          emp_sk,
          short_name,
          sort_name,
          id,
          last_name,
          first_name,
          eff_hire_nom_date,
          term_nom_date,
          active_flag,
          time_zone_sk,
          seniority,
          email_adr,
          memo,
          upd_tcs_login_sk,
          upd_ts,
          im_usr_name,
          tech_datestamp,
          tech_type,
          tech_num,
          row_number () over (partition BY emp_sk
                              ORDER BY jrn_date DESC) rnk
   FROM ${hivevar:PROD_DATABASE}.ewfm_bgs_emp
   WHERE tech_datestamp = '${hivevar:READPARTITION}'
     AND tech_type= 'OPEN'
     AND tech_num= '1') sub_q
WHERE sub_q.rnk = 1;





 -------------------------------------table created for ewfm_bgb_emp----------------------------------------------

DROP TABLE IF EXISTS ${hivevar:DS_DATABASE}.ewfm_bgb_emp;
CREATE TABLE IF NOT EXISTS ${hivevar:DS_DATABASE}.ewfm_bgb_emp
(jrn_date                string,
jrn_flag                string,
tech_closure_flag       string, 
tech_start_date         string,
tech_end_date           string,
emp_sk                  string,
short_name              string,
sort_name               string,
id                      string,
last_name               string,
first_name              string,
eff_hire_nom_date       string,
term_nom_date           string,
active_flag             string,
time_zone_sk            string,
seniority               string,
email_adr               string,
memo                    string,
upd_tcs_login_sk        string,
upd_ts                  string,
im_usr_name             string,
tech_datestamp          string, 
tech_type               string, 
tech_num                string)
STORED AS ORC tblproperties ("orc.compress"="SNAPPY","orc.compress.size"="16384");


INSERT INTO TABLE ${hivevar:DS_DATABASE}.ewfm_bgb_emp
SELECT jrn_date,
       jrn_flag,
       tech_closure_flag,
       tech_start_date,
       tech_end_date,
       emp_sk,
       short_name,
       sort_name,
       id,
       last_name,
       first_name,
       eff_hire_nom_date,
       term_nom_date,
       active_flag,
       time_zone_sk,
       seniority,
       email_adr,
       memo,
       upd_tcs_login_sk,
       upd_ts,
       im_usr_name,
       tech_datestamp,
       tech_type,
       tech_num
FROM
  (SELECT jrn_date,
          jrn_flag,
          tech_closure_flag,
          tech_start_date,
          tech_end_date,
          emp_sk,
          short_name,
          sort_name,
          id,
          last_name,
          first_name,
          eff_hire_nom_date,
          term_nom_date,
          active_flag,
          time_zone_sk,
          seniority,
          email_adr,
          memo,
          upd_tcs_login_sk,
          upd_ts,
          im_usr_name,
          tech_datestamp,
          tech_type,
          tech_num,
          row_number () over (partition BY emp_sk
                              ORDER BY jrn_date DESC) rnk
   FROM ${hivevar:PROD_DATABASE}.ewfm_bgb_emp
   WHERE tech_datestamp = '${hivevar:READPARTITION}'
     AND tech_type= 'OPEN'
     AND tech_num= '1') sub_q
WHERE sub_q.rnk = 1;




-------------------------------------table created for ewfm_bgne_emp----------------------------------------------

DROP TABLE IF EXISTS ${hivevar:DS_DATABASE}.ewfm_bgne_emp;
CREATE TABLE IF NOT EXISTS ${hivevar:DS_DATABASE}.ewfm_bgne_emp
(jrn_date                string,
jrn_flag                string,
tech_closure_flag       string, 
tech_start_date         string,
tech_end_date           string,
emp_sk                  string,
short_name              string,
sort_name               string,
id                      string,
last_name               string,
first_name              string,
eff_hire_nom_date       string,
term_nom_date           string,
active_flag             string,
time_zone_sk            string,
seniority               string,
email_adr               string,
memo                    string,
upd_tcs_login_sk        string,
upd_ts                  string,
im_usr_name             string,
tech_datestamp          string, 
tech_type               string, 
tech_num                string)
STORED AS ORC tblproperties ("orc.compress"="SNAPPY","orc.compress.size"="16384");


INSERT INTO TABLE ${hivevar:DS_DATABASE}.ewfm_bgne_emp
SELECT jrn_date,
       jrn_flag,
       tech_closure_flag,
       tech_start_date,
       tech_end_date,
       emp_sk,
       short_name,
       sort_name,
       id,
       last_name,
       first_name,
       eff_hire_nom_date,
       term_nom_date,
       active_flag,
       time_zone_sk,
       seniority,
       email_adr,
       memo,
       upd_tcs_login_sk,
       upd_ts,
       im_usr_name,
       tech_datestamp,
       tech_type,
       tech_num
FROM
  (SELECT jrn_date,
          jrn_flag,
          tech_closure_flag,
          tech_start_date,
          tech_end_date,
          emp_sk,
          short_name,
          sort_name,
          id,
          last_name,
          first_name,
          eff_hire_nom_date,
          term_nom_date,
          active_flag,
          time_zone_sk,
          seniority,
          email_adr,
          memo,
          upd_tcs_login_sk,
          upd_ts,
          im_usr_name,
          tech_datestamp,
          tech_type,
          tech_num,
          row_number () over (partition BY emp_sk
                              ORDER BY jrn_date DESC) rnk
   FROM ${hivevar:PROD_DATABASE}.ewfm_bgne_emp
   WHERE tech_datestamp = '${hivevar:READPARTITION}'
     AND tech_type= 'OPEN'
     AND tech_num= '1') sub_q
WHERE sub_q.rnk = 1;


