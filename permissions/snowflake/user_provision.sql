-- SET THE FOLLOWING VARIABLES BEFORE THE SCRIPT (with correct values) using the SECURITYADMIN role
-- ====================
-- set (email, firstname, lastname) = ('email@gitlab.com', 'Vovchik', 'Ulyanov');
-- ====================
use role securityadmin;

use warehouse ADMIN;

set username = (select upper(LEFT($email, CHARINDEX('@', $email) - 1)));

set loginname = (select lower($email));

CREATE USER identifier($username) 
LOGIN_NAME = $loginname
DISPLAY_NAME = $username 
FIRST_NAME = $firstname
LAST_NAME = $lastname 
EMAIL = $email;

CREATE ROLE identifier($username) ;
GRANT ROLE identifier($username) TO ROLE "SYSADMIN";

GRANT ROLE identifier($username) to user identifier($username);

-- IF GOING TO BE A DBT USER, run this to create the development databases

set prod_db = (select $username || '_PROD');
set prep_db = (select $username || '_PREP');

use role sysadmin;

CREATE DATABASE identifier($prod_db);
GRANT OWNERSHIP ON DATABASE identifier($prod_db) to role identifier($username);
GRANT ALL PRIVILEGES ON DATABASE identifier($prod_db) to role identifier($username);

CREATE DATABASE identifier($prep_db);
GRANT OWNERSHIP ON DATABASE identifier($prep_db) to role identifier($username);
GRANT ALL PRIVILEGES ON DATABASE identifier($prep_db) to role identifier($username);

use role securityadmin;
