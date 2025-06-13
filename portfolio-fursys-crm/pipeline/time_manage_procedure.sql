CREATE OR REPLACE PROCEDURE ODS.PUBLIC.SP_TIME_MNG(TASK_DIV VARCHAR, S_HOUR FLOAT, E_HOUR FLOAT, S_MINUTE FLOAT)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS CALLER
AS
$$
try {
// Procedure Name : Root Task 프로시저
// Last Update : 2025.02.05
// Content : 최초 작성

	// 타겟 데이터베이스 및 스키마 선택   
    snowflake.createStatement({sqlText: 'USE DATABASE ODS'}).execute();  // 운영 ODS, 개발 ODS_DEV
    snowflake.createStatement({sqlText: 'USE SCHEMA PUBLIC'}).execute();
    
	// 현재 시각 및 시간/분 추출
	var event_timestamp = new Date();
	var event_hour = event_timestamp.getHours();
	var event_minute = event_timestamp.getMinutes();
	
	var returnValue; // 최종 리턴 값 (후행 Task 활성화 여부)
	
	// 지정 시작일시부터 지정 종료일시까지 후행 TASK가 활성화되고
	// 지정 시작일시에는 *후행 TASK가 활성화된 최종 일시*로 후행 프로시저 P_DATE가 설정되도록 하는 로직
	if (event_hour == S_HOUR && event_minute == S_MINUTE) {
    
		// 시작 시간일 때: 단순 리턴 값 설정
        returnValue = 'On schedule'
    } else if ((event_hour == S_HOUR && event_minute == S_MINUTE+30) 
			|| (event_hour > S_HOUR && event_hour < E_HOUR) 
			|| (event_hour == E_HOUR && event_minute == S_MINUTE)	// 자정부터 작업 멈춰야 하므로 E_HOUR(23)까지 작업 수행
			|| (event_hour == E_HOUR && event_minute == S_MINUTE+30)) {		// 자정부터 작업 멈춰야 하므로 E_HOUR(23)까지 작업 수행
    //} else if ((event_hour == S_HOUR && event_minute != S_MINUTE) || (event_hour > S_HOUR && event_hour < E_HOUR)) {
    
		// 지정 구간 내: 작업시간 수정
		returnValue = 'On schedule'
        var updateStatement = snowflake.createStatement({sqlText:`UPDATE ODS.PUBLIC.TIME_MNG SET UPDATE_TIMESTAMP = CURRENT_TIMESTAMP WHERE TASK_DIV = '${TASK_DIV}';`});
        updateStatement.execute();
	} else {
		// 그 외의 경우: 단순 리턴 값 설정
		returnValue = 'Off schedule'
	}
	
	// 후행 Task 실행 여부를 시스템에 전달
	var taskActiveStatement = snowflake.createStatement({sqlText:`CALL system$set_return_value('` + returnValue + `');`});
		taskActiveStatement.execute();
	
	return 'Success';
    
} catch (err) {
    var errorMessage = `${err.message}`;
    return errorMessage;
}
$$;