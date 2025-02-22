 CREATE TABLE HTZ_CUSTOM.XXHTZ_INVOICE_HEADER_STG_TBL 
   (	
   INVOICE_ID                                 	NUMBER, 
	INV_TYPE                                  	VARCHAR2(100), 
	PAYMENT                                   	VARCHAR2(1), 
	PO_HEADER_ID                              	NUMBER, 
	ORG_ID                                 	  	NUMBER, 
	ORG_NAME                                  	VARCHAR2(100), 
	VIN                                 		VARCHAR2(100), 
	INVOICE_NUMBER                              VARCHAR2(100), 
	DEALER_INVOICE_AMT                          NUMBER, 
	SERVICE_VEHICLE_TAX_AMT                     NUMBER, 
	FACT_INVOICE_REBATE_AMT                     NUMBER, 
	INVOICE_DATE                                VARCHAR2(30), 
	PLATE_NUMBER                                VARCHAR2(100), 
	VENDOR_ID                                 	NUMBER, 
	SEGMENT1                                 	VARCHAR2(100), 
	VENDOR_NAME                                 VARCHAR2(100), 
	VENDOR_SITE_CODE                            VARCHAR2(100), 
	WFAPPROVAL_STATUS                           VARCHAR2(100), 
	CHECK_DATE                                 	VARCHAR2(30), 
	REGION                                 		VARCHAR2(10), 
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
	 CONSTRAINT INVOICE_HDR_ID_PK PRIMARY KEY (INVOICE_ID)
    );