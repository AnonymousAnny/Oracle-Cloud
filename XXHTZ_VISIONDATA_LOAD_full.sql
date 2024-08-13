CREATE OR REPLACE PACKAGE xxhtz_visiondata_load AS
/*****************************************************************
OBJECT NAME: Vision Vehicle Details to Oracle
DESCRIPTION: Vision Vehicle Details to Oracle

Version 	Name              	Date           		Description
----------------------------------------------------------------------------
<1.0>		Harshit	      22-JUN-2022 	       	   V1.0- Initial Draft
<2.0>		Rahul	      04-OCT-2022 	       	   V2.0- Changes in Design
*****************************************************************/
    g_err_msg VARCHAR2(2000);
    PROCEDURE insert_data (
        p_oic_instance_id IN NUMBER,
        p_file_name       IN VARCHAR2,
        p_object_number   IN VARCHAR2,
        p_invoked_by      IN VARCHAR2,
        p_status_out      OUT VARCHAR2,
        p_err_msg_out     OUT VARCHAR2
    );

    PROCEDURE update_retention_flag (
        p_status_out  OUT VARCHAR2,
        p_err_msg_out OUT VARCHAR2
    );

    PROCEDURE batch_vs_data (
        p_batch_id    IN NUMBER,
        p_batch_size  IN NUMBER,
        p_status_out  OUT VARCHAR2,
        p_err_msg_out OUT VARCHAR2
    );

    PROCEDURE del_dupl_data (
        p_oracle_inst_id IN NUMBER,
        p_file_name      IN VARCHAR2,
        p_status_out     OUT VARCHAR2,
        p_err_msg_out    OUT VARCHAR2
    );

END xxhtz_visiondata_load;
/


CREATE OR REPLACE PACKAGE BODY XXHTZ_VISIONDATA_LOAD AS
/*****************************************************************
OBJECT NAME: Vision Vehicle Details to Oracle
DESCRIPTION: Vision Vehicle Details to Oracle

Version 	Name              	Date           		Description
----------------------------------------------------------------------------
<1.0>		Harshit	      22-JUN-2022 	       	   V1.0- Initial Draft
<2.0>		Rahul	      04-OCT-2022 	       	   V2.0- Changes in Design
*****************************************************************/


PROCEDURE insert_data( p_oic_instance_id 	IN 	 NUMBER
					 , p_file_name			IN 	 VARCHAR2
					 , p_object_number      IN   VARCHAR2
                     , p_invoked_by         IN   VARCHAR2
                     , p_status_out    	    OUT  VARCHAR2
					 , p_err_msg_out        OUT  VARCHAR2
                     )
IS
	CURSOR c_extract_rec
	IS
		SELECT vehicle_record,
DECODE(TRIM(SUBSTR(VEHICLE_RECORD, 92, 8)),'00000000',NULL,
To_DATE(TRIM(SUBSTR(VEHICLE_RECORD, 92, 8)),'YYYYMMDD')
) VEH_DELIVERY_DATE,
DECODE(TRIM(SUBSTR(VEHICLE_RECORD, 115, 8)),'00000000',NULL,
To_DATE(TRIM(SUBSTR(VEHICLE_RECORD,115, 8)),'YYYYMMDD')
) VEH_SERVICE_DATE
		  FROM xxhtz_po_car_info_stg_tbl
		 WHERE oic_instance_id = p_oic_instance_id
		   ORDER BY record_id;

    CURSOR c_vin_stg_data
    IS
    SELECT country, vin from XXHTZ_PO_CAR_INFO_STG_ALL where oracle_instance_id = p_oic_instance_id;

    TYPE x_vin_stg_cur_type IS TABLE OF c_vin_stg_data%ROWTYPE INDEX BY PLS_INTEGER;
    x_vin_stg_cur_tbl x_vin_stg_cur_type;


	TYPE x_car_stg_tab_typ 	IS TABLE OF XXHTZ_PO_CAR_INFO_ALL%ROWTYPE INDEX BY PLS_INTEGER;

    type t_car_org is record
    (
    Org_Id varchar2(50)
    );
    TYPE x_car_del_type IS TABLE OF t_car_org INDEX BY PLS_INTEGER;
    x_car_del_org_tbl x_car_del_type;
    j      NUMBER := 0;

	x_idx 			    PLS_INTEGER DEFAULT 0;
	x_car_stg_tab 		x_car_stg_tab_typ;
	x_invoice_num       VARCHAR2 (50);
    x_vendor_name       VARCHAR2 (240);
    x_vendor_num        VARCHAR2 (240);
	x_source_exist_flag VARCHAR2 (1);
    x_invoice_id        NUMBER;
	x_invoice_line_id	NUMBER;
    x_gl_date           DATE;
	x_source            VARCHAR2 (80);
	v_org_id            VARCHAR2(100) := NULL;
	counter             NUMBER := 0;
    temp_org_id         VARCHAR2(100) := NULL;
    temp_cntrycode      NUMBER := NULL;
	v_license_count     NUMBER := 0;
	old_org             NUMBER := NULL;
	v_file_name         VARCHAR2 (240);

	BEGIN
        p_err_msg_out := 'N';
		--Loop all records
		FOR rec IN c_extract_rec
		LOOP

		x_idx 												:= x_idx + 1;
        x_car_stg_tab( x_idx ).oracle_instance_id			:= p_oic_instance_id ;
--		x_car_stg_tab( x_idx ).record_id					:= x_idx;
        x_car_stg_tab( x_idx ).COUNTRY					    := TRIM(SUBSTR(rec.VEHICLE_RECORD, 1,2));
		x_car_stg_tab( x_idx ).UNIT					        := TRIM(SUBSTR(rec.VEHICLE_RECORD, 3, 10));
		x_car_stg_tab( x_idx ).LICENSE					    := TRIM(SUBSTR(rec.VEHICLE_RECORD, 13,10));
		x_car_stg_tab( x_idx ).VIN					        := TRIM(SUBSTR(rec.VEHICLE_RECORD, 23,18));
		x_car_stg_tab( x_idx ).MODEL_YR					    := TRIM(SUBSTR(rec.VEHICLE_RECORD, 41, 2));
		x_car_stg_tab( x_idx ).VEH_DESC					    := TRIM(SUBSTR(rec.VEHICLE_RECORD, 43, 5));
		x_car_stg_tab( x_idx ).MODEL				     	:= TRIM(SUBSTR(rec.VEHICLE_RECORD, 48, 20));
		x_car_stg_tab( x_idx ).MILEAGE				     	:= TRIM(SUBSTR(rec.VEHICLE_RECORD, 68, 7));
		x_car_stg_tab( x_idx ).CV_INDICATOR					:= TRIM(SUBSTR(rec.VEHICLE_RECORD, 75, 1));
		x_car_stg_tab( x_idx ).AREA_NUM				        := TRIM(SUBSTR(rec.VEHICLE_RECORD, 76, 5));
		x_car_stg_tab( x_idx ).TYPE_CODE				    := TRIM(SUBSTR(rec.VEHICLE_RECORD, 81, 1));
		x_car_stg_tab( x_idx ).SAFETY_RECALL				:= TRIM(SUBSTR(rec.VEHICLE_RECORD, 82, 10));
		x_car_stg_tab( x_idx ).DELIVERY_DATE				:= rec.VEH_DELIVERY_DATE;  --To_Date(TRIM(SUBSTR(rec.VEHICLE_RECORD, 92, 8)),'YYYYMMDD');
		x_car_stg_tab( x_idx ).MDL_CODE					    := TRIM(SUBSTR(rec.VEHICLE_RECORD, 100, 4));
		x_car_stg_tab( x_idx ).RTE_CLS					    := TRIM(SUBSTR(rec.VEHICLE_RECORD, 104, 3));
		x_car_stg_tab( x_idx ).ENGINE_SIZE					:= TRIM(SUBSTR(rec.VEHICLE_RECORD, 107, 6));
        x_car_stg_tab( x_idx ).STATUS_CODE					:= TRIM(SUBSTR(rec.VEHICLE_RECORD, 113, 1));
        x_car_stg_tab( x_idx ).SERVICE_DATE					:= rec.VEH_SERVICE_DATE;  --TRIM(SUBSTR(rec.VEHICLE_RECORD, 115, 8));
		x_car_stg_tab( x_idx ).FILE_NAME					:= p_file_name;
		x_car_stg_tab( x_idx ).OBJECT_NUMBER				:= p_object_number;
        x_car_stg_tab( x_idx ).last_update_date		        := SYSDATE;
        x_car_stg_tab( x_idx ).created_by                   := p_invoked_by;
        x_car_stg_tab( x_idx ).creation_date                := SYSDATE;
        x_car_stg_tab( x_idx ).last_updated_by              := p_invoked_by;


		v_file_name := p_file_name;


		 IF (x_car_stg_tab( x_idx ).CV_INDICATOR != 'C' AND x_car_stg_tab( x_idx ).CV_INDICATOR != 'V')
         THEN
             x_car_stg_tab( x_idx ).CV_INDICATOR := NULL;
         END IF;

		BEGIN



		SELECT Meaning into v_org_id
		FROM xxhtz_fah_fnd_lookup_values
		WHERE lookup_Type = 'HERTZ_VISION_LOAD_ORG_ID'
		AND LOOKUP_CODE =  x_car_stg_tab( x_idx ).COUNTRY;
        EXCEPTION
		WHEN OTHERS THEN
       g_err_msg := ' Warning - Lookup Value - '||x_car_stg_tab( x_idx ).COUNTRY||' is not present in Table HERTZ_VISION_LOAD_ORG_ID - '||SUBSTR(SQLERRM, 1, 500);
       p_status_out := g_err_msg;
       --p_err_msg_out := 'Y';
		END;

        IF  x_car_stg_tab( x_idx ).COUNTRY IN
                                             ('A2',
                                              'NZ',
                                              'AU',
                                              'HA',
                                              'HF',
                                              'HL',
                                              'HN',
                                              'CN',
                                              'BJ',
                                              'SH',
                                              'SA',
                                              'AA',
                                              'AN',
                                              'BE',
                                              'FR',
                                              'F3',
                                              'G2',
                                              'GE',
                                              'IT',
                                              'I2',
                                              'LU',
                                              'NE',
                                              'NF',
                                              'SP',
                                              'SZ',
                                              'UK')
                     THEN
                        x_car_stg_tab(x_idx).ORG_ID	:= v_org_id;
        END IF;
        x_car_del_org_tbl(j).Org_Id := v_org_id;

        j:= j+1;
      END LOOP;

        BEGIN
	  	/* Stagging Table Insertion */
		FORALL i IN x_car_stg_tab.FIRST .. x_car_stg_tab.LAST
		INSERT INTO XXHTZ_PO_CAR_INFO_STG_ALL
             VALUES
			   x_car_stg_tab(i);
		      COMMIT;
     EXCEPTION
		WHEN OTHERS THEN
       g_err_msg := 'Error is occured at inserting data into XXHTZ_PO_CAR_INFO_STG_ALL staging Table  - '||SQLCODE||' '||SUBSTR(SQLERRM, 1, 500);
       p_status_out := g_err_msg;
       p_err_msg_out := 'Y';
		END;


	  /* Compare main table and delete the data from main table */
	  	BEGIN

        OPEN c_vin_stg_data;
        loop
            fetch c_vin_stg_data BULK COLLECT INTO x_vin_stg_cur_tbl LIMIT 10000;

            IF x_vin_stg_cur_tbl.COUNT > 0
            THEN

                FORALL j IN x_vin_stg_cur_tbl.first..x_vin_stg_cur_tbl.last
                update XXHTZ_PO_CAR_INFO_ALL
                set record_status = 'X'
                where vin = x_vin_stg_cur_tbl(j).vin
                and country = x_vin_stg_cur_tbl(j).country;

            END IF;

        exit when c_vin_stg_data%NOTFOUND;

        end loop;

        close c_vin_stg_data;

		Delete from XXHTZ_PO_CAR_INFO_ALL xpo
		/* Below WHERE Condition is added */
					WHERE 1=1
					AND record_status = 'X';

		commit;
	     EXCEPTION
		WHEN OTHERS THEN
       g_err_msg := 'Error is occured at deleting data from XXHTZ_PO_CAR_INFO_ALL Table  - '||SQLCODE||' '||SUBSTR(SQLERRM, 1, 500);
       p_status_out := g_err_msg;
       p_err_msg_out := 'Y';
		END;

	/* Inserting the data from stg table to main table */

		BEGIN

		IF p_err_msg_out <> 'Y'
		THEN

			 INSERT INTO xxhtz_po_car_info_all
			  SELECT *
			  FROM xxhtz_po_car_info_stg_all;

			commit;
		END IF;

		EXCEPTION
		WHEN OTHERS THEN
	   g_err_msg := 'Error is occured at insertion data on xxhtz_po_car_info_all Table  - '||SQLCODE||' '||SUBSTR(SQLERRM, 1, 500);
       p_status_out := g_err_msg;
       p_err_msg_out := 'Y';
		END;


	/* Deleting the data from stg table*/

	BEGIN
		IF p_err_msg_out <> 'Y'
		THEN

		DELETE FROM xxhtz_po_car_info_stg_all where oracle_instance_id = p_oic_instance_id;
        DELETE FROM xxhtz_po_car_info_stg_tbl where oic_instance_id = p_oic_instance_id;

		commit;
		END IF;
       	EXCEPTION
		WHEN OTHERS THEN
	   g_err_msg := 'Error is occured at deleting data from xxhtz_po_car_info_stg_all Table  - '||SQLCODE||' '||SUBSTR(SQLERRM, 1, 500);
       p_status_out := g_err_msg;
       p_err_msg_out := 'Y';
		END;


	/* Inserting data into XXHTZ_VIN_VS_LOAD_STG Table for infleet and defleet  */
	BEGIN
    --IF SUBSTR(p_file_name,18,2) = 'XX'  --commented after full load 1/29 and adhoc run of 2/17
    IF SUBSTR(p_file_name,18,2) = '01'
	THEN
    FORALL i IN x_car_stg_tab.FIRST .. x_car_stg_tab.LAST

   INSERT INTO HTZ_CUSTOM.XXHTZ_VIN_VS_LOAD_STG
								(
								 BATCH_ID
								,VS_VALUE
								,COUNTRY
								,ACTION
								,STATUS
								,ERR_MESSAGE
								,CREATED_BY
								,CREATION_DATE
								,LAST_UPDATED_BY
								,LAST_UPDATE_DATE
								,ORACLE_INSTANCE_ID
								)
								VALUES
								(
								 x_car_stg_tab(i).oracle_instance_id
								,x_car_stg_tab(i).VIN||'-'||x_car_stg_tab(i).LICENSE
								,x_car_stg_tab(i).COUNTRY
								,x_car_stg_tab(i).STATUS_CODE -- Coming from File
								,'N'
								,NULL
								,p_invoked_by
								,SYSDATE
								,p_invoked_by
								,SYSDATE
                                ,NULL
								);

	 END IF;

	COMMIT;
 	EXCEPTION
		WHEN OTHERS THEN
	   g_err_msg := 'Error is occured at inserting data on XXHTZ_VIN_VS_LOAD_STG Table  - '||SQLCODE||' '||SUBSTR(SQLERRM, 1, 500);
       p_status_out := g_err_msg;
       p_err_msg_out := 'Y';
		END;



/*
	  FORALL i IN x_car_del_org_tbl.FIRST .. x_car_del_org_tbl.LAST
		DELETE FROM XXHTZ_PO_CAR_INFO_ALL
              Where ORG_ID = x_car_del_org_tbl(i).Org_Id;  */
/*
         IF  x_car_stg_tab( x_idx ).COUNTRY IN
                                             ('A2',
                                              'NZ',
                                              'AU',
                                              'HA',
                                              'HF',
                                              'HL',
                                              'HN',
                                              'CN',
                                              'BJ',
                                              'SH',
                                              'SA',
                                              'AA',
                                              'AN',
                                              'BE',
                                              'FR',
                                              'F3',
                                              'G2',
                                              'GE',
                                              'IT',
                                              'I2',
                                              'LU',
                                              'NE',
                                              'NF',
                                              'SP',
                                              'SZ',
                                              'UK')
                     THEN

                        x_car_stg_tab(x_idx).ORG_ID	:= v_org_id;

                        IF v_org_id IS NOT NULL
                        THEN
                           IF ( (counter = 0) OR (v_org_id <> temp_org_id))
                           THEN
                              IF (v_org_id <> temp_org_id)
                              THEN
                                 old_org := temp_org_id;
                                 COMMIT WORK;

                                 counter := 0;
                              END IF;

                              temp_org_id := v_org_id;

                              --DELETE FROM hertz_car_info_all
                              DELETE FROM XXHTZ_PO_CAR_INFO_ALL
                                    WHERE org_id = v_org_id;

                           END IF;

                           IF temp_org_id = v_org_id
                           THEN
                              BEGIN
									SAVEPOINT start_transaction;

                                 BEGIN
                                    v_license_count := 0;

                                    SELECT COUNT (1)
                                      INTO v_license_count
                                      FROM XXHTZ_PO_CAR_INFO_ALL --R12 IC Changes
                                     WHERE org_id = v_org_id
                                           AND license = RTRIM (x_car_stg_tab( x_idx ).LICENSE)
                                           AND NVL (
                                                  TO_DATE (x_car_stg_tab( x_idx ).DELIVERY_DATE,
                                                           'RRRRMMDD'),
                                                  TRUNC (SYSDATE)) >
                                                  NVL (delivery_date,
                                                       TRUNC (SYSDATE));

                                    IF v_license_count > 0
                                    THEN
                                       DELETE FROM XXHTZ_PO_CAR_INFO_ALL
                                             WHERE org_id = v_org_id
                                                   AND license =
                                                          RTRIM (x_car_stg_tab(x_idx).LICENSE);

                                    END IF;
                                 EXCEPTION
                                    WHEN OTHERS
                                    THEN
                                      raise_application_error( -20001, 'Error Encountered in Function replace_junk_chars. String: ' || SQLERRM );
                                  END;
							   END;
							END IF;
                    END IF;
		   END IF;
        END LOOP;

*/


/*
		FORALL i IN x_car_stg_tab.FIRST .. x_car_stg_tab.LAST
		INSERT INTO XXHTZ_PO_CAR_INFO_ALL
             VALUES
			   x_car_stg_tab(i);
		      Commit;
						*/

EXCEPTION
		WHEN OTHERS THEN
	   g_err_msg := 'Error is occured at main procedure  - '||SQLCODE||' '||SUBSTR(SQLERRM, 1, 500);
       p_status_out := g_err_msg;
       p_err_msg_out := 'Y';

END insert_data;

PROCEDURE update_retention_flag(  p_status_out    	    OUT  VARCHAR2
                            	 , p_err_msg_out        OUT  VARCHAR2)
   IS

BEGIN

      UPDATE XXHTZ_PO_CAR_INFO_ALL
         SET data_retention_flag = 'Y'
       WHERE unit IN
                (
                 SELECT unit
                   FROM XXHTZ_PO440_RETENTION_FLAG
                )
             AND license IN
                    (
					 SELECT LICENSE
                     FROM XXHTZ_PO440_RETENTION_FLAG
					);


      COMMIT;


      DELETE from XXHTZ_PO440_RETENTION_FLAG;

      Commit;
EXCEPTION
   		WHEN OTHERS THEN
	   g_err_msg := 'Error is occured on procedure update_retention_flag   - '||SQLCODE||' '||SUBSTR(SQLERRM, 1, 500);
       p_status_out := g_err_msg;
       p_err_msg_out := 'Y';
End update_retention_flag;

PROCEDURE batch_vs_data (  p_batch_id		IN NUMBER
                         , p_batch_size		IN NUMBER
						 , p_status_out		OUT VARCHAR2
						 , p_err_msg_out	OUT VARCHAR2)
IS

	CURSOR c_vs_batch_i
	IS
	WITH vs_val AS ( SELECT ROWNUM rn, xx.vs_value, xx.action
					   FROM XXHTZ_VIN_VS_LOAD_STG xx
				      WHERE batch_id = p_batch_id
						AND action = 'I'),
		   grps AS ( SELECT r.*,
							ceil ( rn / p_batch_size ) batch_no
					   FROM   vs_val r )
	SELECT batch_no, grps.vs_value, grps.action
	  FROM grps
	 GROUP BY batch_no, vs_value, action;

	CURSOR c_vs_batch_d
	IS
	WITH vs_val AS ( SELECT ROWNUM rn, xx.vs_value, xx.action
					   FROM XXHTZ_VIN_VS_LOAD_STG xx
				      WHERE batch_id = p_batch_id
						AND action = 'D'),
		   grps AS ( SELECT r.*,
							ceil ( rn / p_batch_size ) batch_no
					   FROM   vs_val r )
	SELECT batch_no, grps.vs_value, grps.action
	  FROM grps
	 GROUP BY batch_no, vs_value, action;

	 l_rec_cnt_i NUMBER;
	 l_rec_cnt_d NUMBER;

	 TYPE vs_batch_type IS TABLE OF c_vs_batch_i%ROWTYPE;
	 vs_batch_i_tbl vs_batch_type;
	 vs_batch_d_tbl vs_batch_type;

BEGIN

	SELECT COUNT(1)
	  INTO l_rec_cnt_i
	  FROM XXHTZ_VIN_VS_LOAD_STG
	 WHERE batch_id = p_batch_id
	   AND action = 'I';

	 SELECT COUNT(1)
	  INTO l_rec_cnt_d
	  FROM XXHTZ_VIN_VS_LOAD_STG
	 WHERE batch_id = p_batch_id
	   AND action = 'D';

	 IF l_rec_cnt_i > 0
	 THEN

		OPEN c_vs_batch_i;
		FETCH c_vs_batch_i BULK COLLECT INTO vs_batch_i_tbl;
		CLOSE c_vs_batch_i;

		FORALL i IN vs_batch_i_tbl.FIRST..vs_batch_i_tbl.LAST
		UPDATE XXHTZ_VIN_VS_LOAD_STG
		   SET batch_no = vs_batch_i_tbl(i).batch_no
		 WHERE 1=1
		   AND batch_id = p_batch_id
		   AND vs_value = vs_batch_i_tbl(i).vs_value
		   AND action = 'I';

	 END IF;

	 IF l_rec_cnt_d > 0
	 THEN

		OPEN c_vs_batch_d;
		FETCH c_vs_batch_d BULK COLLECT INTO vs_batch_d_tbl;
		CLOSE c_vs_batch_d;

		FORALL j IN vs_batch_d_tbl.FIRST..vs_batch_d_tbl.LAST
		UPDATE XXHTZ_VIN_VS_LOAD_STG
		   SET batch_no = vs_batch_d_tbl(j).batch_no
		 WHERE 1=1
		   AND batch_id = p_batch_id
		   AND vs_value = vs_batch_d_tbl(j).vs_value
		   AND action = 'D';

	 END IF;

	 COMMIT;

EXCEPTION
	WHEN OTHERS THEN
	   g_err_msg := 'Error is occured on procedure batch_vs_data   - '||SQLCODE||' '||SUBSTR(SQLERRM, 1, 500);
       p_status_out := g_err_msg;
       p_err_msg_out := 'Y';

END batch_vs_data;

PROCEDURE del_dupl_data (  p_oracle_inst_id IN NUMBER
						 , p_file_name		IN VARCHAR2
						 , p_status_out		OUT VARCHAR2
						 , p_err_msg_out	OUT VARCHAR2)
IS

	CURSOR dupl_defl_data
	IS
	SELECT vin, country, COUNT(1) cnt
	  FROM xxhtz_po_car_info_all
	 WHERE 1=1
	   AND oracle_instance_id = p_oracle_inst_id
	 GROUP BY vin, country HAVING COUNT(1) > 1;		-- All the VIN in a batch with duplicate records

	l_rec_count NUMBER;

	TYPE xx_car_info_type IS TABLE OF dupl_defl_data%ROWTYPE;
	xx_car_info_tbl xx_car_info_type;

BEGIN

	IF SUBSTR(p_file_name,18,2) = '01'
	THEN

		OPEN  dupl_defl_data;
		FETCH dupl_defl_data BULK COLLECT INTO xx_car_info_tbl;
		CLOSE dupl_defl_data;

		IF xx_car_info_tbl.COUNT > 0
		THEN

			FOR i IN xx_car_info_tbl.FIRST..xx_car_info_tbl.LAST
			LOOP

				l_rec_count := 0;

				SELECT COUNT(1)
				  INTO l_rec_count
				  FROM xxhtz_po_car_info_all
				 WHERE 1=1
				   AND oracle_instance_id = p_oracle_inst_id
				   AND vin = xx_car_info_tbl(i).vin
				   AND country = xx_car_info_tbl(i).country
				   AND license = '99';								-- Check if any record with License plate 99

				IF l_rec_count > 0
				THEN

					DELETE FROM xxhtz_po_car_info_all
					 WHERE 1=1
					   AND oracle_instance_id = p_oracle_inst_id
					   AND vin = xx_car_info_tbl(i).vin
					   AND country = xx_car_info_tbl(i).country
					   AND license = '99'
					   AND ROWNUM = 1;								-- Delete te record with licese 99 if exists

				ELSE

					DELETE FROM xxhtz_po_car_info_all
					 WHERE 1=1
					   AND oracle_instance_id = p_oracle_inst_id
					   AND vin = xx_car_info_tbl(i).vin
					   AND country = xx_car_info_tbl(i).country
					   AND status_code = 'D'
					   AND ROWNUM = 1;								-- Delete defleet if license with 99 doesnt exists.

				END IF;

			END LOOP;

		END IF;

	END IF;

    COMMIT;

EXCEPTION
	WHEN OTHERS THEN
	   g_err_msg := 'Error is occured on procedure del_dupl_data   - '||SQLCODE||' '||SUBSTR(SQLERRM, 1, 500);
       p_status_out := g_err_msg;
       p_err_msg_out := 'Y';
END del_dupl_data;

END XXHTZ_VISIONDATA_LOAD;
/
