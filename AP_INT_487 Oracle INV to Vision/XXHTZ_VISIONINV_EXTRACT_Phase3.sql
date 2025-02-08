CREATE OR REPLACE PACKAGE xxhtz_visioninv_extract AS
  /**************************************************************************
  *

  * DESCRIPTION
  *  This package compares onhand quantity from Korber to EBS.
  *
  *

  * HISTORY
  * =======
  *
  * VERSION DATE         AUTHOR(S)       DESCRIPTION
  * ------- -----------  --------------- ------------------------------------
  * 1.0     22-Jul-2022  Srinivas P   Intial creation
  * 1.1     14-Jun-2023  Utsab Ghosh  INC001567831
  * 1.2     22-Jun-2023  Utsab Ghosh  INC001628675
  *************************************************************************/
    FUNCTION format_amount (
        amount IN NUMBER
    ) RETURN VARCHAR;

    PROCEDURE main (
        p_oic_instance_id IN NUMBER,
		p_bu_name         IN VARCHAR2,
        p_errbuf          OUT VARCHAR2,
        p_retcode         OUT NUMBER,
		p_offset          IN NUMBER,
		p_limit           IN NUMBER
    );


END xxhtz_visioninv_extract;
/


CREATE OR REPLACE PACKAGE BODY xxhtz_visioninv_extract AS
  /**************************************************************************
  *

  * DESCRIPTION
  *  This package compares onhand quantity from Korber to EBS.
  *
  *

  * HISTORY
  * =======
  *
  * VERSION DATE         AUTHOR(S)       DESCRIPTION
  * ------- -----------  --------------- ------------------------------------
  * 1.0     22-Jul-2022  Srinivas P   Intial creation
  * 1.1     14-Jul-2023  Utsab Ghosh  INC001567831
  * 1.2     02-Aug-2023  Revati Korde  INC001682023
  * 1.3     08-Sep-2023  Vijay Kumar  INC001662455
  *************************************************************************/
 --Below function is used to format amounts
    FUNCTION format_amount (
        amount IN NUMBER
    ) RETURN VARCHAR IS

        dot_pos           NUMBER;
        v_length          NUMBER;
        char_amount       VARCHAR2(100) := to_char(amount);
        lst_char          VARCHAR2(2);
        formated_lst_char VARCHAR2(2);
        formated_amt      VARCHAR2(20);
        v_sign            NUMBER;
    BEGIN
        SELECT
            sign(amount)
        INTO v_sign
        FROM
            dual;

        IF v_sign = 1 OR v_sign = 0 OR v_sign IS NULL THEN
            IF amount = 0 OR amount IS NULL THEN
                RETURN ( '0000000000{' );
            ELSE
                SELECT
                    instr(char_amount, '.')
                INTO dot_pos
                FROM
                    dual;

                SELECT
                    length(char_amount)
                INTO v_length
                FROM
                    dual;

                IF dot_pos = 0 THEN
                    formated_amt := char_amount || '0{';
                ELSE
                    IF ( v_length - dot_pos ) = 1 THEN
                        SELECT
                            substr(char_amount, 1, dot_pos - 1)
                            || substr(char_amount, dot_pos + 1, v_length - dot_pos)
                            || '{'
                        INTO formated_amt
                        FROM
                            dual;

                    ELSIF ( v_length - dot_pos ) = 2 THEN
                        SELECT
                            substr(char_amount, - 1, 1)
                        INTO lst_char
                        FROM
                            dual;

                        SELECT
                            chr(ascii('A') - 1 + lst_char)
                        INTO formated_lst_char
                        FROM
                            dual;

                        SELECT
                            substr(char_amount, 1, dot_pos - 1)
                            || substr(char_amount, dot_pos + 1, 1)
                            || formated_lst_char
                        INTO formated_amt
                        FROM
                            dual;

                    END IF;
                END IF;

            END IF;

        ELSIF v_sign = -1 THEN
            SELECT
                substr(to_char(amount), 2, length(to_char(amount)))
            INTO char_amount
            FROM
                dual;

            SELECT
                instr(char_amount, '.')
            INTO dot_pos
            FROM
                dual;

            SELECT
                length(char_amount)
            INTO v_length
            FROM
                dual;

            IF dot_pos = 0 THEN
                formated_amt := char_amount || '0}';
            ELSE
                IF ( v_length - dot_pos ) = 1 THEN
                    SELECT
                        substr(char_amount, 1, dot_pos - 1)
                        || substr(char_amount, dot_pos + 1, v_length - dot_pos)
                        || '}'
                    INTO formated_amt
                    FROM
                        dual;

                ELSIF ( v_length - dot_pos ) = 2 THEN
                    SELECT
                        substr(char_amount, - 1, 1)
                    INTO lst_char
                    FROM
                        dual;

                    SELECT
                        chr(ascii('J') - 1 + lst_char)
                    INTO formated_lst_char
                    FROM
                        dual;

                    SELECT
                        substr(char_amount, 1, dot_pos - 1)
                        || substr(char_amount, dot_pos + 1, 1)
                        || formated_lst_char
                    INTO formated_amt
                    FROM
                        dual;

                END IF;
            END IF;

        END IF;

        RETURN ( formated_amt );
    END format_amount;
        
 --Below is the main procedure called from integration
    PROCEDURE main (
        p_oic_instance_id IN NUMBER,
		p_bu_name         IN VARCHAR2,
        p_errbuf          OUT VARCHAR2,
        p_retcode         OUT NUMBER,
	    p_offset          IN NUMBER,
		p_limit           IN NUMBER
    ) AS

        v_vin_length       NUMBER;
        v_vin_alpha_check  NUMBER;
        v_error_flag       VARCHAR2(1);
        v_err_msg          VARCHAR2(2000);
        v_sent_status      VARCHAR2(20);
        l_tag              VARCHAR2(30);
        x_inv_exists       NUMBER;
        v_cancel_flag      VARCHAR2(1);
        v_excep1           VARCHAR2(3000) := 'Invoices set to Error status in current instance of program';
        v_excep2           VARCHAR2(200) := 'Invoices set to Manual status in current instance of program';
        v_excep3           VARCHAR2(200) := 'Credit notes set to Manual status in current instance of program';
        v_pre_org          NUMBER;
        v_pre_comp_code    VARCHAR2(100);
        v_comp_code        VARCHAR2(100);
        v_old_po_header_id NUMBER := NULL;
        v_old_po_num       VARCHAR2(100) := NULL;
        v_prev_line_num    NUMBER;
        v_po_num           VARCHAR2(20);
        l_po_match_once    VARCHAR2(255);
        v_code             VARCHAR2(5);
        v_interface_source VARCHAR2(50);
        v_inv_status_code  VARCHAR2(5);
        v_po_area_num      VARCHAR2(50);
        v_po_number        VARCHAR2(100);
        v_line_count       NUMBER;
				 -- bucket variables
        l_bo               NUMBER;
        l_dt               NUMBER;
        l_ad               NUMBER;
        l_hb               NUMBER;
        l_fi               NUMBER;
        l_fp               NUMBER;
        l_ic               NUMBER;
        l_mt               NUMBER;
        l_lt               NUMBER;
        l_mc2              NUMBER;
        l_md               NUMBER;
        CURSOR cur_hdr IS
        SELECT DISTINCT
            hdr.plate_number,
            hdr.vendor_id,
            hdr.segment1,
            hdr.vendor_name,
            hdr.vendor_site_code,
            hdr.wfapproval_status,
            hdr.check_date,
            hdr.invoice_id,
            hdr.inv_type,
            hdr.payment,
            hdr.po_header_id,
            hdr.org_id,
            hdr.org_name,
            hdr.vin,
            hdr.invoice_number,
            hdr.dealer_invoice_amt,
            hdr.service_vehicle_tax_amt,
            hdr.fact_invoice_rebate_amt,
            hdr.invoice_date,
            ext.sent_status,
            hdr.region,
            hdr.OIC_INSTANCE_ID
        FROM
            xxhtz_invoice_header_stg_tbl hdr,
            xxhtz_ap_vision_extract_tbl  ext
        WHERE
            hdr.invoice_id = ext.invoice_id (+)
			AND hdr.region=p_bu_name ---(Region Wise Segregation)
            AND nvl(ext.sent_status, 'New') <> 'Paid'
            AND nvl(ext.sent_status, 'New') <> 'Manual'
            AND (hdr.oic_instance_id = p_oic_instance_id or ext.sent_status is null) -- header oic_instance is added
						--AND  HDR.INVOICE_ID='300000006451781'
        ORDER BY
            hdr.invoice_id
			OFFSET p_offset row fetch first p_limit rows only;

        CURSOR cur_lines (
            p_invoice_id NUMBER,
            in_oic_instance_id NUMBER
        ) IS
        SELECT Distinct  --added for 1.2
            invoice_id,
            ccid,
            amount,
            discount,
            inv_line_type,
            conv_po_num,
            org_id,
            line_type,
            po_distribution_id,
            line_number,
            segment5,
            segment6,
            segment1,
            po_header_id,
            po_segment1,
            cancel_flag,
            interface_source_code,
            lookup_code,
            tag
        FROM
            xxhtz_invoice_lines_stg_tbl
        WHERE
            invoice_id = p_invoice_id
			and OIC_INSTANCE_ID = in_oic_instance_id  ---(OIC Instance Id is added)
		--AND nvl(cancelled_flag,'N') <> 'Y'
        ORDER BY
            invoice_id,
            line_number;

    BEGIN
        FOR rec_cur_hdr IN cur_hdr LOOP
            --dbms_output.put_line('Invoice Number: ' || rec_cur_hdr.invoice_number);
            v_error_flag := 'N';
            v_err_msg := NULL;
			v_po_number:=NULL;
            SELECT
                COUNT(1)
            INTO x_inv_exists
            FROM
                xxhtz_ap_vision_extract_tbl
            WHERE
                invoice_id = rec_cur_hdr.invoice_id;

            IF x_inv_exists = 0 THEN
                BEGIN
                    INSERT INTO xxhtz_ap_vision_extract_tbl (
                        invoice_id,
                        invoice_number,
                        invoice_date,
                        dealer_invoice_amt,
                        inv_type,
                        payment_flag,
                        vendor_id,
                        vendor_number,
                        vendor_site_code,
                        vendor_name,
                        po_header_id,
                        org_id,
                        organization,--Added,17Nov22
                        vin,
                        region,
                        payment_date,
                        plate_number,
                        oic_instance_id,
                        creation_date,
                        created_by,
                        last_update_date
                    ) VALUES (
					  -- null,--REC_CUR_HDR.sent_status,
                        rec_cur_hdr.invoice_id,
                        rec_cur_hdr.invoice_number,
                        rec_cur_hdr.invoice_date,
                        rec_cur_hdr.dealer_invoice_amt,
                        rec_cur_hdr.inv_type,
                        rec_cur_hdr.payment,
                        rec_cur_hdr.vendor_id,
                        rec_cur_hdr.segment1,
                        rec_cur_hdr.vendor_site_code,
                        rec_cur_hdr.vendor_name,
                        rec_cur_hdr.po_header_id,
                        rec_cur_hdr.org_id,
                        rec_cur_hdr.org_name,
                        rec_cur_hdr.vin,
                        rec_cur_hdr.region,
                        rec_cur_hdr.check_date,
                        rec_cur_hdr.plate_number,
                        p_oic_instance_id,
                        sysdate,
                        - 1,
                        sysdate
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                    null;
                        --dbms_output.put_line('Error while inserting for Invoive#: ' || rec_cur_hdr.invoice_number);
                END;
                --start of added on 8 Dec
            ELSE
               IF
                rec_cur_hdr.sent_status is null
                 THEN
                    update xxhtz_ap_vision_extract_tbl
                        set oic_instance_id=p_oic_instance_id,
                        sent_status='N',
                        error_desc=null
                        , last_update_date=sysdate --added on 16 dec
                         WHERE
                            invoice_id = rec_cur_hdr.invoice_id;

               END IF;
               --end of added on 8 Dec
        END IF;

            IF
                nvl(rec_cur_hdr.sent_status, 'New') = 'Error'
                AND rec_cur_hdr.payment = 'N'--IF1
            THEN
                NULL;
            ELSE

	-- Validations...start
                IF rec_cur_hdr.dealer_invoice_amt < 0 THEN
                    UPDATE xxhtz_ap_vision_extract_tbl
                    SET
                        sent_status = 'Manual',
                        error_flag = 'Y',
                        error_desc = v_excep3
                        , last_update_date=sysdate --added on 16 dec
                    WHERE
                            invoice_id = rec_cur_hdr.invoice_id
                        AND rec_cur_hdr.dealer_invoice_amt < 0;

                ELSE --If inv amount>0

                    BEGIN --B1
                     --v_new_status := NULL;
					 --- initialising bucket variables
                        l_bo := 0;
                        l_dt := 0;
                        l_ad := 0;
                        l_hb := 0;
                        l_fi := 0;
                        l_fp := 0;
                        l_ic := 0;
                        l_mt := 0;
                        l_lt := 0;
                        l_mc2 := 0;
                        l_md := 0;
                        v_pre_org := NULL;
                        v_pre_comp_code := NULL;
                        v_old_po_header_id := NULL;
                        v_old_po_num := NULL; --5.0
                        IF
                            nvl(rec_cur_hdr.sent_status, 'New') = 'Sent'--IF2
                            AND rec_cur_hdr.payment = 'N'
                        THEN
                            l_bo := 0;
                        ELSE
                            BEGIN
                                SELECT
                                    nvl(length(TRIM(translate(rec_cur_hdr.vin, 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890',
                                    ' '))), 0)
                                INTO v_vin_alpha_check
                                FROM
                                    dual;

                                SELECT
                                    length(rec_cur_hdr.vin)
                                INTO v_vin_length
                                FROM
                                    dual;

                                IF v_vin_length <> 17 OR v_vin_alpha_check <> 0 THEN
                                    v_error_flag := 'Y';
                                    v_err_msg := 'Invoice number '
                                                 || rec_cur_hdr.invoice_number
                                                 || '--VIN is not in correct format ';
                                    UPDATE xxhtz_ap_vision_extract_tbl
                                    SET
                                        error_flag = 'Y',
                                        error_desc = v_err_msg
                                    WHERE
                                        invoice_id = rec_cur_hdr.invoice_id;

                                    --dbms_output.put_line('VIN Validation failed for Invoice Number: ' || rec_cur_hdr.invoice_number);
                                    /*dbms_output.put_line('v_vin_length: '
                                                         || v_vin_length
                                                         || '-------'
                                                         || 'v_vin_alpha_check: '
                                                         || v_vin_alpha_check);*/

                                END IF;

                            EXCEPTION
                                WHEN OTHERS THEN
                                null;
                                    --dbms_output.put_line('Error while validating VIN number for Invoive: ' || rec_cur_hdr.invoice_number);
                            END;

                            v_prev_line_num := 0;
                            IF v_error_flag <> 'Y' THEN
                            -- Filter Added for sent status at line level processing
								If (rec_cur_hdr.sent_status is null or rec_cur_hdr.sent_status in ('N','Error')) then
                                FOR rec_cur_lines IN cur_lines(rec_cur_hdr.invoice_id,rec_cur_hdr.oic_instance_id) LOOP
                                    -- v_err_msg := NULL;
                                    v_comp_code := rec_cur_lines.segment1;
                                    IF (
                                        nvl(v_pre_org, rec_cur_lines.org_id) = rec_cur_lines.org_id
                                        AND nvl(v_pre_comp_code, v_comp_code) = v_comp_code
                                    ) THEN
                                        IF rec_cur_lines.line_type <> 'TAX' --AND v_correct = TRUE
                                         THEN
                                            BEGIN
									  --5.0
                                                IF rec_cur_lines.po_distribution_id IS NULL THEN
                                                    v_po_num := rec_cur_lines.conv_po_num;
											  -- v_cancel_flag := 'N';
                                                    IF rec_cur_lines.conv_po_num IS NOT NULL THEN
                                                        l_po_match_once := 'Y';
                                                    END IF;
                                                    IF nvl(v_old_po_num, v_po_num) <> v_po_num THEN
                                                        v_err_msg := 'Invoice number '  --issue
                                                                     || rec_cur_hdr.invoice_number
																--|| '  Vendor Name '
																--|| v_vendor_name1
                                                                     || '  Organization id '
                                                                     || rec_cur_hdr.org_id
                                                                     || '---Matched with multiple PO ';

                                                        v_error_flag := 'Y';
                                                        --dbms_output.put_line('Validation for Inv Line number: ' || rec_cur_lines.line_number);
                                                        --dbms_output.put_line('Error Msg: ' || v_err_msg);
                                                    END IF;

                                                    v_old_po_num := v_po_num;
                                                ELSE
									  -- 5.0

                                                    BEGIN
                                                        v_interface_source := rec_cur_lines.interface_source_code;
                                                        IF rec_cur_lines.po_header_id IS NOT NULL THEN
                                                            l_po_match_once := 'Y';
                                                        END IF;
                                                        IF nvl(v_old_po_header_id, rec_cur_lines.po_header_id) <> rec_cur_lines.po_header_id
                                                        THEN
                                                            v_err_msg := 'Invoice number '  --issue
                                                                         || rec_cur_hdr.invoice_number
                                                --|| '  Vendor Name '
                                               -- || v_vendor_name1
                                                                         || '  Organization id '
                                                                         || rec_cur_hdr.org_id
                                                                         || '--Matched with multiple PO ';

                                                            v_error_flag := 'Y';
                                                            --dbms_output.put_line('Validation for Inv Line number: ' || rec_cur_lines.line_number);
                                                            --dbms_output.put_line('Error Msg: ' || v_err_msg);
                                                        END IF;

                                                        v_old_po_header_id := rec_cur_lines.po_header_id;
                                                        IF v_cancel_flag = 'Y'
                                          ---  validation for match with cancelled PO
                                                         THEN
                                                            v_err_msg := 'Invoice number '  --issue
                                                                         || rec_cur_hdr.invoice_number
                                               -- || '  Vendor Name '
                                              --  || v_vendor_name1
                                                                         || '  Organization id '
                                                                         || rec_cur_hdr.org_id
                                                                         || '--PO  is cancelled ';

                                                            v_error_flag := 'Y';
                                                            --dbms_output.put_line('Validation for Inv Line number: ' || rec_cur_lines.line_number);
                                                            --dbms_output.put_line('Error Msg: ' || v_err_msg);
                                                        END IF;

                                                    EXCEPTION
                                                        WHEN no_data_found THEN
                                                        --dbms_output.put_line('Error Msg: ' || 'test');
                                                            NULL;
                                                    END;
                                                END IF; --5.0

                                                v_code := rec_cur_lines.tag;
                                                v_po_area_num := rec_cur_lines.lookup_code;
                                                IF v_error_flag <> 'Y' THEN
                                                    IF rec_cur_lines.line_number <> v_prev_line_num THEN
                                                        CASE
                                                            WHEN v_code = 'AD' THEN
                                                                l_ad := l_ad + nvl(rec_cur_lines.amount, 0) + nvl(rec_cur_lines.discount,
                                                                0);

                                                                --dbms_output.put_line('IN AD CASE: ' || l_ad);
                                                            WHEN v_code = 'BO' THEN
                                                                l_bo := l_bo + nvl(rec_cur_lines.amount, 0) + nvl(rec_cur_lines.discount,
                                                                0);
                                                            WHEN v_code = 'DT' THEN
                                                                l_dt := l_dt + nvl(rec_cur_lines.amount, 0) + nvl(rec_cur_lines.discount,
                                                                0);
                                                            WHEN v_code = 'MC2' THEN
                                                                l_mc2 := l_mc2 + nvl(rec_cur_lines.amount, 0) + nvl(rec_cur_lines.discount,
                                                                0);
                                                            WHEN v_code = 'FI' THEN
                                                                l_fi := l_fi + nvl(rec_cur_lines.amount, 0) + nvl(rec_cur_lines.discount,
                                                                0);
                                                            WHEN v_code = 'FP' THEN
                                                                l_fp := l_fp + nvl(rec_cur_lines.amount, 0) + nvl(rec_cur_lines.discount,
                                                                0);
                                                            WHEN v_code = 'HB' THEN
                                                                l_hb := l_hb + nvl(rec_cur_lines.amount, 0) + nvl(rec_cur_lines.discount,
                                                                0);
                                                            WHEN v_code = 'IC' THEN
                                                                l_ic := l_ic + nvl(rec_cur_lines.amount, 0) + nvl(rec_cur_lines.discount,
                                                                0);
                                                            WHEN v_code = 'LT' THEN
                                                                l_lt := l_lt + nvl(rec_cur_lines.amount, 0) + nvl(rec_cur_lines.discount,
                                                                0);
                                                            WHEN v_code = 'MC2' THEN
                                                                l_mc2 := l_mc2 + nvl(rec_cur_lines.amount, 0) + nvl(rec_cur_lines.discount,
                                                                0);
                                                            WHEN v_code = 'MD' THEN
                                                                l_md := l_md + nvl(rec_cur_lines.amount, 0) + nvl(rec_cur_lines.discount,
                                                                0);
                                                            WHEN v_code = 'MT' THEN
                                                                l_mt := l_mt + nvl(rec_cur_lines.amount, 0) + nvl(rec_cur_lines.discount,
                                                                0);
                                                            WHEN v_code = 'MC2' THEN
                                                                l_mc2 := l_mc2 + nvl(rec_cur_lines.amount, 0) + nvl(rec_cur_lines.discount,
                                                                0);
                                                            ELSE
                                                                -- NULL; --Commented by Kunal 11/24
                                                                v_error_flag := 'Y';
                                                                v_err_msg := v_err_msg ||';' || 'Bucket code not found in bucket mapping table.';
                                                        END CASE;

                                                    ELSE
                                                        CASE
                                                            WHEN v_code = 'AD' THEN
                                                                l_ad := l_ad + nvl(rec_cur_lines.amount, 0);
                                                            WHEN v_code = 'BO' THEN
                                                                l_bo := l_bo + nvl(rec_cur_lines.amount, 0);
                                                            WHEN v_code = 'DT' THEN
                                                                l_dt := l_dt + nvl(rec_cur_lines.amount, 0);
                                                            WHEN v_code = 'MC2' THEN
                                                                l_mc2 := l_mc2 + nvl(rec_cur_lines.amount, 0);
                                                            WHEN v_code = 'FI' THEN
                                                                l_fi := l_fi + nvl(rec_cur_lines.amount, 0);
                                                            WHEN v_code = 'FP' THEN
                                                                l_fp := l_fp + nvl(rec_cur_lines.amount, 0);
                                                            WHEN v_code = 'HB' THEN
                                                                l_hb := l_hb + nvl(rec_cur_lines.amount, 0);
                                                            WHEN v_code = 'IC' THEN
                                                                l_ic := l_ic + nvl(rec_cur_lines.amount, 0);
                                                            WHEN v_code = 'LT' THEN
                                                                l_lt := l_lt + nvl(rec_cur_lines.amount, 0);
                                                            WHEN v_code = 'MC2' THEN
                                                                l_mc2 := l_mc2 + nvl(rec_cur_lines.amount, 0);
                                                            WHEN v_code = 'MD' THEN
                                                                l_md := l_md + nvl(rec_cur_lines.amount, 0);
                                                            WHEN v_code = 'MT' THEN
                                                                l_mt := l_mt + nvl(rec_cur_lines.amount, 0);
                                                            WHEN v_code = 'MC2' THEN
                                                                l_mc2 := l_mc2 + nvl(rec_cur_lines.amount, 0);
                                                            ELSE
                                                                -- NULL; --Commented by Kunal 11/24
                                                                v_error_flag := 'Y';
                                                                v_err_msg := v_err_msg ||';' || 'Bucket code not found in bucket mapping table.';
                                                        END CASE;
                                                    END IF;
                                       --R12 IC changes for incident INC000037239 ends
                                                END IF;

                                            END;
                                        END IF;

                                        v_pre_org := rec_cur_lines.org_id;
                                        v_pre_comp_code := rec_cur_lines.segment1;--v_comp_code;
                                    ELSE
                                        v_err_msg := v_err_msg ||';' || 'distribution lines having different company code for '
                                                     || 'Invoice number '               --issue
                                                     || rec_cur_hdr.invoice_number
                                   -- || '  Vendor Name '
                                   -- || v_vendor_name1
                                                     || '  Organization id '
                                                     || rec_cur_hdr.org_id;

                                        v_error_flag := 'Y';
                                    END IF;               -- end for  same org check

                                    v_prev_line_num := rec_cur_lines.line_number;
                                END LOOP;
								END If;

                                IF l_po_match_once = 'N'
                           -- added by joy das on 4th sept .

                                 THEN
                                    v_err_msg := v_err_msg ||';' ||
                                    -- validation for distribution lines from different ORG
                                     'None of the item lines is matched to PO for  '
                                                 || 'Invoice number '                  --issue
                                                 || rec_cur_hdr.invoice_number
                                -- || '  Vendor Name '
                               --  || v_vendor_name1
                                                 || '  Organization id '
                                                 || rec_cur_hdr.org_id;

                                    v_error_flag := 'Y';
                                END IF;

                                IF v_error_flag <> 'Y' THEN
		                          /*  UPDATE XXHTZ_AP_VISION_EXTRACT_TBL
                                    SET sent_status = 'Sent'
                                  WHERE invoice_id = REC_CUR_HDR.invoice_id;
		                         */

                                    IF rec_cur_hdr.sent_status = 'Sent' THEN
                                        v_inv_status_code := 'P';
                                    END IF;
                                    IF rec_cur_hdr.sent_status IS NULL THEN
                                        v_inv_status_code := 'A';
                                    END IF;
                                    IF
                                        rec_cur_hdr.sent_status = 'Error'
                                        AND rec_cur_hdr.payment = 'Y'
                                    THEN
                                        v_inv_status_code := 'A';
                                    END IF;
------
                                    IF
                                        rec_cur_hdr.sent_status = 'Error'
                                        AND rec_cur_hdr.payment = 'N'
                                    THEN
                                        v_inv_status_code := 'A';
                                    END IF;

                                    IF
                                        rec_cur_hdr.sent_status IS NULL
                                        AND rec_cur_hdr.payment = 'Y'
                                    THEN
                                        UPDATE xxhtz_ap_vision_extract_tbl
                                        SET
                                            sent_status = 'Paid'
                                            ,payment_date = rec_cur_hdr.check_date--added , 17-Nov
                                            , payment_flag=rec_cur_hdr.payment                                             
                                            , last_update_date=sysdate --added on 16 dec
                                        WHERE
                                            invoice_id = rec_cur_hdr.invoice_id;
									-- v_new_status := 'Sent';
                                    END IF;

                                    IF
                                        rec_cur_hdr.sent_status IS NULL
                                        AND rec_cur_hdr.payment = 'N'
                                    THEN
                                        UPDATE xxhtz_ap_vision_extract_tbl
                                        SET
                                            sent_status = 'Sent'
                                            , last_update_date=sysdate --added on 16 dec
                                        WHERE
                                            invoice_id = rec_cur_hdr.invoice_id;
                                -- v_new_status := 'Sent';
                                    END IF;

                                    IF
                                        rec_cur_hdr.sent_status = 'Sent' 
                                        AND rec_cur_hdr.payment = 'Y'
                                    THEN
                                        UPDATE xxhtz_ap_vision_extract_tbl
                                        SET
                                            sent_status = 'Paid',
                                            oic_instance_id = p_oic_instance_id--added , 17-Nov
                                            ,
                                            payment_date = rec_cur_hdr.check_date--added , 17-Nov
                                            , payment_flag=rec_cur_hdr.payment  --added on 16 dec
                                            , last_update_date=sysdate --added on 16 dec
                                        WHERE
                                            invoice_id = rec_cur_hdr.invoice_id;
                               --  v_new_status := 'Paid';
                                    END IF;

                                    IF
                                        rec_cur_hdr.sent_status = 'Error'
                                        AND rec_cur_hdr.payment = 'Y'
                                    THEN
                                        UPDATE xxhtz_ap_vision_extract_tbl
                                        SET
                                            sent_status = 'Sent'
                                            , last_update_date=sysdate --added on 16 dec
                                        WHERE
                                            invoice_id = rec_cur_hdr.invoice_id;
                              -- End modification for IC R12
                               --  v_new_status := 'Sent';
                                    END IF;

                                    IF
                                        rec_cur_hdr.sent_status = 'Error'
                                        AND rec_cur_hdr.payment = 'N'
                                    THEN
                                        UPDATE xxhtz_ap_vision_extract_tbl
                                        SET
                                            sent_status = 'Sent'
                                            , last_update_date=sysdate --added on 16 dec
                                        WHERE
                                            invoice_id = rec_cur_hdr.invoice_id;
                              -- End modification for IC R12
                                -- v_new_status := 'Sent';
                                    END IF;
                                --IF Block added as per ERP-433
                                    IF upper(nvl(v_interface_source, '*')) = 'ARIBA' THEN
                                        v_po_num := substr(v_po_num, 3);
                                    END IF;

                                    BEGIN
                                        --dbms_output.put_line('Inside format amounts update: ');
										If (rec_cur_hdr.sent_status is null or rec_cur_hdr.sent_status in ('N','Error')) then
                                        UPDATE xxhtz_ap_vision_extract_tbl xx
                                        SET
                                            xx.inv_status_code = v_inv_status_code,
                                            xx.po_number = v_po_num,
                                            xx.frm_dealer_invoice_amt = format_amount(rec_cur_hdr.dealer_invoice_amt),
                                            xx.frm_service_vehicle_tax_amt = format_amount(rec_cur_hdr.service_vehicle_tax_amt),
                                            xx.frm_fact_invoice_rebate_amt = format_amount(rec_cur_hdr.fact_invoice_rebate_amt),
                                            xx.bo = l_bo,
                                            xx.dt = l_dt,
                                            xx.ad = l_ad,
                                            xx.hb = l_hb,
                                            xx.fi = l_fi,
                                            xx.fp = l_fp,
                                            xx.mc2 = l_mc2,
                                            xx.mt = l_mt,
                                            xx.lt = l_lt,
                                            xx.md = l_md,
                                            xx.ic = abs(l_ic),
                                            xx.frm_bo = nvl(format_amount(l_bo), 0),
                                            xx.frm_dt = nvl(format_amount(l_dt), 0),
                                            xx.frm_ad = nvl(format_amount(l_ad), 0),
                                            xx.frm_hb = nvl(format_amount(l_hb), 0),
                                            xx.frm_fi = nvl(format_amount(l_fi), 0),
                                            xx.frm_fp = nvl(format_amount(l_fp), 0),
                                            xx.frm_mc2 = nvl(format_amount(l_mc2), 0),
                                            xx.frm_mt = nvl(format_amount(l_mt), 0),
                                            xx.frm_lt = nvl(format_amount(l_lt), 0),
                                            xx.frm_md = nvl(format_amount(l_md), 0),
                                            xx.frm_ic = nvl(format_amount(l_ic), 0),
                                            xx.sum_of_amounts = nvl(format_amount(l_bo + l_ad + l_dt + l_mc2 + l_fp + l_fi + l_hb), 0),
                                            xx.format_zero = format_amount(0),
                                            xx.area_nummber = nvl(v_po_area_num, '00000')
                                        WHERE
                                            invoice_id = rec_cur_hdr.invoice_id;
										else
										UPDATE xxhtz_ap_vision_extract_tbl xx
                                        SET
                                            xx.inv_status_code = v_inv_status_code
										WHERE
                                            invoice_id = rec_cur_hdr.invoice_id;
										end if;

                                        --dbms_output.put_line('Number of records updated :' || SQL%rowcount);
                                    EXCEPTION
                                        WHEN OTHERS THEN
                                        null;
                                            /*dbms_output.put_line('Error while updating vision extract table:invoice_id: '
                                                                 || rec_cur_hdr.invoice_id
                                                                 || '--'
                                                                 || sqlerrm);*/
                                    END;

                                ELSE --v_error_flag =Y', Error records
                                    BEGIN
                                        IF
                                            rec_cur_hdr.sent_status IS NULL
                                            AND rec_cur_hdr.payment = 'Y'
                                        THEN
                                            UPDATE xxhtz_ap_vision_extract_tbl
                                            SET
                                                sent_status = 'Manual',
                                                error_flag = 'Y',
                                                -- error_desc = v_excep2
                                                error_desc = v_err_msg
                                                , last_update_date=sysdate --added on 16 dec
                                            WHERE
                                                invoice_id = rec_cur_hdr.invoice_id;
											   --  v_error_reason := v_excep2;
												-- v_new_status := 'Manual';
                                        ELSIF
                                            nvl(rec_cur_hdr.sent_status, 'New') = 'Error'
                                            AND rec_cur_hdr.payment = 'Y'
                                        THEN
												 -- if invoice  alredy paid and vision _sent_status sent then it will not fall in any case
                                            UPDATE xxhtz_ap_vision_extract_tbl
                                            SET
                                                sent_status = 'Manual',
                                                error_flag = 'Y',
                                                -- error_desc = v_excep2
                                                error_desc = v_err_msg
                                                , last_update_date=sysdate --added on 16 dec
                                            WHERE
                                                invoice_id = rec_cur_hdr.invoice_id;
												  --  v_error_reason := v_excep2;
												   -- v_new_status := 'Manual';
                                        ELSIF
                                            nvl(rec_cur_hdr.sent_status, 'New') = 'Sent'
                                            AND rec_cur_hdr.payment = 'Y'
                                        THEN
                                            UPDATE xxhtz_ap_vision_extract_tbl
                                            SET
                                                sent_status = 'Manual',
                                                error_flag = 'Y',
                                                -- error_desc = v_excep2
                                                error_desc = v_err_msg
                                                , last_update_date=sysdate --added on 16 dec
                                            WHERE
                                                invoice_id = rec_cur_hdr.invoice_id;
													   --v_error_reason := v_excep2;
													  -- v_new_status := 'Manual';
                                        ELSE
                                            UPDATE xxhtz_ap_vision_extract_tbl
                                            SET
                                                sent_status = 'Error',
                                                error_flag = 'Y',
                                                -- error_desc = v_excep1
                                                error_desc = v_err_msg
                                                , last_update_date=sysdate --added on 16 dec
                                            WHERE
                                                invoice_id = rec_cur_hdr.invoice_id;
													   --v_error_reason := v_excep1;
													  -- v_new_status := 'Error';
                                        END IF;

                                    END;
                                END IF;
										--  END;
				--	END IF;
                            END IF; --vin
                        END IF; --IF2
                    END;--B1
                END IF;	--inv amount >0
            END IF;  --IF1
  -- Validations...end
  ----18Nov---start---
            BEGIN
                SELECT DISTINCT
                    po_segment1
                INTO v_po_number
                FROM
                    xxhtz_invoice_lines_stg_tbl
                WHERE
                        invoice_id = rec_cur_hdr.invoice_id
                    AND line_type = 'ITEM'
					AND PO_SEGMENT1 IS NOT NULL;

				SELECT DISTINCT
                    lookup_code
                INTO v_po_area_num
                FROM
                    xxhtz_invoice_lines_stg_tbl
                WHERE
                    invoice_id = rec_cur_hdr.invoice_id
					AND lookup_code IS NOT NULL;

            EXCEPTION
                WHEN OTHERS THEN
                    v_po_number := NULL;
                    --dbms_output.put_line('Multiple POs for invoice: ' || rec_cur_hdr.invoice_id);
            END;

            IF v_po_number IS NOT NULL THEN
                UPDATE xxhtz_ap_vision_extract_tbl
                SET
                    po_number = v_po_number
                WHERE
                    invoice_id = rec_cur_hdr.invoice_id;

            END IF;
            IF v_po_area_num IS NOT NULL THEN
                UPDATE xxhtz_ap_vision_extract_tbl
                SET
                    area_nummber = v_po_area_num
                WHERE
                    invoice_id = rec_cur_hdr.invoice_id;

            END IF;
  ----18Nov---end---
        END LOOP; --Header

        For rec_ext_cur in (select invoice_id,area_nummber,po_number,error_desc from xxhtz_ap_vision_extract_tbl where last_update_date = sysdate and error_flag <> 'Y' and region= p_bu_name) loop
            Select count(*) into v_line_count from xxhtz_invoice_lines_stg_tbl where
            invoice_id = rec_ext_cur.invoice_id and OIC_INSTANCE_ID = p_oic_instance_id;

            if v_line_count = 0 then
                UPDATE xxhtz_ap_vision_extract_tbl
                SET
                    sent_status = null,
                    error_desc = 'No Lines Data imported to ATP tables'
                WHERE
                    invoice_id = rec_ext_cur.invoice_id;
            else 
                If rec_ext_cur.area_nummber is null or rec_ext_cur.area_nummber = '00000'  then
                    UPDATE xxhtz_ap_vision_extract_tbl
                    SET
                        sent_status = null,
                        error_desc = 'Area Number Missing'
                    WHERE
                        invoice_id = rec_ext_cur.invoice_id;
                    commit;
                end if;
                if rec_ext_cur.po_number is null or rec_ext_cur.po_number like '0000%' then
                    UPDATE xxhtz_ap_vision_extract_tbl
                    SET
                        sent_status = null,
                        error_desc = rec_ext_cur.error_desc || 'PO Number Missing'
                    WHERE
                        invoice_id = rec_ext_cur.invoice_id;
                    commit;
                end if;
            end if;
        end loop;

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
        null;
            --dbms_output.put_line('Unknown Exception raised in MAIN Procedure: ' || sqlerrm);
    END main;

END xxhtz_visioninv_extract;
/
