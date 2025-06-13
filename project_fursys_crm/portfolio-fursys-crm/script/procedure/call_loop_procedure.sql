CREATE OR REPLACE PROCEDURE ODS.PUBLIC.SP_CLPRCD_LSTDT("T_PRCD" VARCHAR(16777216), "S_DATE" VARCHAR(16777216), "E_DATE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
// Procedure Name : 매월 말일 프로시저 호출 루프
// Last Update : 2025.01.07
// Content : 최초 작성

// 타겟 데이터베이스 및 스키마 선택   
    snowflake.createStatement({sqlText: 'USE DATABASE ODS'}).execute();  // 운영 ODS, 개발 ODS
    snowflake.createStatement({sqlText: 'USE SCHEMA PUBLIC'}).execute();

    var event_timestamp = E_DATE;
    var today = new Date();
    var year = today.getFullYear();
    var month = ('0' + (today.getMonth() + 1)).slice(-2);  // 월은 0부터 시작하므로 +1
    var day = ('0' + today.getDate()).slice(-2);

    // 'YYYY-MM-DD' 형식으로 변환
    var work_date = `${year}-${month}-${day}`;
    
    var target_database = '루프 실행 프로시저';
    var target_table = 'T_PRCD';
    var source_database = 'ODS.PUBLIC';
    var source_table = 'SP_CLPRCD_LSTDT';
	var category = 'ODS';

// 프로시저 실행 쿼리 생성
    let calls = []; // 테스트 출력 확인용 배열
	var startDate = new Date(S_DATE);
	var endDate = new Date(E_DATE);
		
	while (startDate <= endDate) {
		var s_yyyy = startDate.getFullYear()
        var s_mm = startDate.getMonth()+1
		var str_s_mm = ('0' + (startDate.getMonth() + 1)).slice(-2);
        var lastDayOfMonth = new Date(s_yyyy, s_mm, 0).getDate();
        var s_yyyymm01 = new Date(s_yyyy, s_mm, 1);

		if (s_yyyymm01 < endDate) {
			var callQuery = `CALL ` + T_PRCD + `('` + s_yyyy + `-` + str_s_mm + `-` + lastDayOfMonth + `')`;

			var callStatement = snowflake.createStatement({sqlText: callQuery});
		      callStatement.execute();
            calls.push(callQuery);
		}

		startDate.setMonth(startDate.getMonth() + 1);
		startDate.setDate(1);
	}

	var finalCallQuery = `CALL ` + T_PRCD + `('` + E_DATE + `')`;
	var finalCallStatement = snowflake.createStatement({sqlText: finalCallQuery});
	finalCallStatement.execute();
    calls.push(finalCallQuery);
    return calls
    //return 'Success'
    
    
} catch (err) {
    var errorMessage = `${err.message}`;
    
    return errorMessage;
}
$$;