CREATE OR REPLACE PROCEDURE ODS.PUBLIC.GET_ALL_PROCEDURE_DDL(P_LIKE VARCHAR, PRMTR VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS $$

	var ddlList = []
	snowflake.createStatement({sqlText: 'USE DATABASE ODS'}).execute();  // 운영 ODS, 개발 ODS_DEV
    snowflake.createStatement({sqlText: 'USE SCHEMA PUBLIC'}).execute();
    
    // 스키마의 모든 테이블 조회
    var listQuery = `SHOW PROCEDURES LIKE '` + P_LIKE + `' IN ODS.PUBLIC`;

    var procedures = snowflake.createStatement({sqlText: listQuery}).execute();

    //var totalDeleted = 0;

    // 프로시저 생성문 추출
    while (procedures.next()) {
        var procedureName = procedures.getColumnValue(2);
        var getDdlQuery = `SELECT GET_DDL('PROCEDURE','` + procedureName + `(` + PRMTR + `)')
                          `;

        try {
            var stmt = snowflake.createStatement({sqlText: getDdlQuery});

            var result = stmt.execute();
            result.next();
            var ddlStatement = result.getColumnValue(1);
			ddlList.push(ddlStatement)

        } catch (err) {

            continue;

        }
    }

    return ddlList;
$$;