create or replace PACKAGE xxhtz_visioninv_extract AS
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
  *************************************************************************/
    FUNCTION format_amount (
        amount IN NUMBER
    ) RETURN VARCHAR;

    PROCEDURE main (
        p_oic_instance_id IN NUMBER,
		p_bu_name         IN VARCHAR2,
        p_errbuf          OUT VARCHAR2,
        p_retcode         OUT NUMBER
    );

END xxhtz_visioninv_extract;