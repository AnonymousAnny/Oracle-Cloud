WHENEVER SQLERROR EXIT SQL.SQLCODE;

alter table XXHTZ_AP_VISION_BACKLOGS_TBL  add OIC_INSTANCE_ID1 varchar2(100);
update XXHTZ_AP_VISION_BACKLOGS_TBL  set OIC_INSTANCE_ID1 = OIC_INSTANCE_ID;
commit;
alter table XXHTZ_AP_VISION_BACKLOGS_TBL drop column OIC_INSTANCE_ID;
alter table XXHTZ_AP_VISION_BACKLOGS_TBL rename column OIC_INSTANCE_ID1 to OIC_INSTANCE_ID;
--------------------------------------------------------------------------------
alter table XXHTZ_INVOICE_HEADER_STG_TBL add OIC_INSTANCE_ID1 varchar2(100);
update XXHTZ_INVOICE_HEADER_STG_TBL set OIC_INSTANCE_ID1 = OIC_INSTANCE_ID;
commit;
alter table XXHTZ_INVOICE_HEADER_STG_TBL drop column OIC_INSTANCE_ID;
alter table XXHTZ_INVOICE_HEADER_STG_TBL rename column OIC_INSTANCE_ID1 to OIC_INSTANCE_ID;
--------------------------------------------------------------------------------
alter table XXHTZ_INVOICE_LINES_STG_TBL add OIC_INSTANCE_ID1 varchar2(100);
update XXHTZ_INVOICE_LINES_STG_TBL set OIC_INSTANCE_ID1 = OIC_INSTANCE_ID;
commit;
alter table XXHTZ_INVOICE_LINES_STG_TBL drop column OIC_INSTANCE_ID;
alter table XXHTZ_INVOICE_LINES_STG_TBL rename column OIC_INSTANCE_ID1 to OIC_INSTANCE_ID;
--------------------------------------------------------------------------------
alter table XXHTZ_AP_VISION_EXTRACT_TBL add OIC_INSTANCE_ID1 varchar2(100);
update XXHTZ_AP_VISION_EXTRACT_TBL set OIC_INSTANCE_ID1 = OIC_INSTANCE_ID;
commit;
alter table XXHTZ_AP_VISION_EXTRACT_TBL drop column OIC_INSTANCE_ID;
alter table XXHTZ_AP_VISION_EXTRACT_TBL rename column OIC_INSTANCE_ID1 to OIC_INSTANCE_ID;