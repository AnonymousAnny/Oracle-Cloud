--------------------------------------------------------
--  File created - Friday-October-18-2024   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Package XXHTZ_VISIONINV_EXTRACT
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "HTZ_CUSTOM"."XXHTZ_VISIONINV_EXTRACT" AS
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
  * 1.3     04-Sep-2024  Anwesha Jena  OCA-221-datatype of oic_instance_id changed to VARCHAR2
  *************************************************************************/
    FUNCTION format_amount (
        amount IN NUMBER
    ) RETURN VARCHAR;

    PROCEDURE main (
        --p_oic_instance_id IN NUMBER,--Commented as part of V 1.3
        p_oic_instance_id IN VARCHAR2,--Added as part of V 1.3
		p_bu_name         IN VARCHAR2,
        p_errbuf          OUT VARCHAR2,
        p_retcode         OUT NUMBER,
		p_offset          IN NUMBER,
		p_limit           IN NUMBER
    );


END xxhtz_visioninv_extract;

/

  GRANT DEBUG ON "HTZ_CUSTOM"."XXHTZ_VISIONINV_EXTRACT" TO "HTZ_READ_ROLE";
