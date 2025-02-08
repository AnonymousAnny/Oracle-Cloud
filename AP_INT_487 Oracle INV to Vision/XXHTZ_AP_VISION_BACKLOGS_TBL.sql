 CREATE TABLE "HTZ_CUSTOM"."XXHTZ_AP_VISION_BACKLOGS_TBL" 
   (	"OIC_INSTANCE_ID" NUMBER, 
	"SENT_VISION" VARCHAR2(2 BYTE) COLLATE "USING_NLS_COMP", 
	"FILE_NAME" VARCHAR2(150 BYTE) COLLATE "USING_NLS_COMP", 
	"INVOICE_NUMBER" VARCHAR2(150 BYTE) COLLATE "USING_NLS_COMP", 
	"VIN" VARCHAR2(150 BYTE) COLLATE "USING_NLS_COMP", 
	"REGION" VARCHAR2(150 BYTE) COLLATE "USING_NLS_COMP", 
	"LOAD_STATUS" VARCHAR2(1 BYTE) COLLATE "USING_NLS_COMP", 
	"ERROR_MESSAGE" VARCHAR2(500 BYTE) COLLATE "USING_NLS_COMP", 
	"CREATION_DATE" DATE, 
	"CREATED_BY" VARCHAR2(100 BYTE) COLLATE "USING_NLS_COMP", 
	"LAST_UPDATE_DATE" DATE, 
	"LAST_UPDATED_BY" VARCHAR2(100 BYTE) COLLATE "USING_NLS_COMP"
   );
