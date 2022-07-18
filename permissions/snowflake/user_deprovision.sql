-- using the SECURITYADMIN role run the following by replacing 'USER_NAME' provided in
-- Offboarding or Deprovisioning issue request.
alter user if
exists user_name set disabled
= true
;
