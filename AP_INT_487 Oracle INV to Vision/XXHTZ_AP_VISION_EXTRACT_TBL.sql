 CREATE TABLE HTZ_CUSTOM.XXHTZ_AP_VISION_EXTRACT_TBL 
   (	
    RECORD_ID 									NUMBER GENERATED BY DEFAULT AS IDENTITY, 
	INVOICE_ID                                  NUMBER, 
	INVOICE_NUMBER                              VARCHAR2(150), 
	INVOICE_DATE                                VARCHAR2(10),	
	DEALER_INVOICE_AMT                          NUMBER, 
	INV_TYPE                              		VARCHAR2(25), 
	PAYMENT_FLAG                                VARCHAR2(1), 
	VENDOR_ID                                 	NUMBER, 
	VENDOR_NUMBER                               VARCHAR2(30), 
	VENDOR_NAME                                 VARCHAR2(100), 
	PO_HEADER_ID                                NUMBER, 
	ORG_ID                                 		NUMBER, 
	ORGANIZATION                                VARCHAR2(100), 
	VIN                                			VARCHAR2(150), 
	SENT_STATUS                                 VARCHAR2(20), 
	SERVICE_VEHICLE_TAX_AMT                     NUMBER, 
	FACT_INVOICE_REBATE_AMT                     NUMBER, 
	PLATE_NUMBER                                VARCHAR2(40), 
	FRM_DEALER_INVOICE_AMT                      VARCHAR2(100), 
	FRM_SERVICE_VEHICLE_TAX_AMT                 VARCHAR2(100), 
	FRM_FACT_INVOICE_REBATE_AMT                 VARCHAR2(100), 
	INV_STATUS_CODE                             VARCHAR2(5), 
	PO_NUMBER                                 	VARCHAR2(100), 
	BO                                 			NUMBER, 
	DT                                 			NUMBER, 
	AD                                 			NUMBER, 
	HB                                 			NUMBER, 
	FI                                 			NUMBER, 
	FP                                 			NUMBER, 
	MC2                                			NUMBER, 
	MT                                 			NUMBER, 
	LT                                 			NUMBER, 
	MD                                 			NUMBER, 
	IC                                 			NUMBER, 
	FRM_BO                                 		VARCHAR2(100), 
	FRM_DT                                 		VARCHAR2(100), 
	FRM_AD                                 		VARCHAR2(100), 
	FRM_HB                                 		VARCHAR2(100), 
	FRM_FI                                 		VARCHAR2(100), 
	FRM_FP                                 		VARCHAR2(100), 
	FRM_MC2                                		VARCHAR2(100), 
	FRM_MT                                 		VARCHAR2(100), 
	FRM_LT                                 		VARCHAR2(100), 
	FRM_MD                                 		VARCHAR2(100), 
	FRM_IC                                 		VARCHAR2(100), 
	AREA_NUMMBER                                VARCHAR2(100), 
	PAYMENT_DATE                                VARCHAR2(30), 
	SUM_OF_AMOUNTS                              VARCHAR2(100), 
	FORMAT_ZERO                                 VARCHAR2(100), 
	REGION                                 		VARCHAR2(10), 
	ERROR_FLAG                                 	VARCHAR2(1) DEFAULT 'N', 
	ERROR_DESC                                 	VARCHAR2(2000), 
	OIC_INSTANCE_ID                             NUMBER, 
	ATTRIBUTE1                                 	VARCHAR2(240), 
	ATTRIBUTE2                                 	VARCHAR2(240), 
	ATTRIBUTE3                                 	VARCHAR2(240), 
	ATTRIBUTE4                                 	VARCHAR2(240), 
	ATTRIBUTE5                                 	VARCHAR2(240), 
	CREATION_DATE 								DATE DEFAULT SYSDATE, 
	CREATED_BY                                 	NUMBER, 
	LAST_UPDATE_DATE 							DATE, 
	LAST_UPDATED_BY                             NUMBER, 
	LAST_UPDATE_LOGIN                           NUMBER, 
	VENDOR_SITE_CODE                            VARCHAR2(100), 
	 CONSTRAINT INVOICE_ID_PK PRIMARY KEY (INVOICE_ID)
   );
  CREATE INDEX HTZ_CUSTOM.XXHTZ_AP_VISION_EXTRACT_TBL_N1 ON HTZ_CUSTOM.XXHTZ_AP_VISION_EXTRACT_TBL
   (INVOICE_ID, SENT_STATUS);