CREATE OR REPLACE PROCEDURE SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA.PROFILING_YYE()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS $$

	// 변수 정의
	var targetDatabase = 'SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS'
	var targetSchema = 'GRANDATA'
	var targetTable = 'PROFILING'
	var sourceDatabase = 'SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS'
	var sourceSchema = 'GRANDATA'
	
	var valueCount = 10
	
	
    // 스키마의 모든 테이블 조회
    var listColumnsQuery = `SELECT TABLE_CATALOG, 
									TABLE_SCHEMA, 
									TABLE_NAME, 
									COLUMN_NAME, 
									DATA_TYPE, 
									IS_NULLABLE, 
									CHARACTER_MAXIMUM_LENGTH, 
									NUMERIC_PRECISION, 
									NUMERIC_SCALE, 
									ORDINAL_POSITION,
									COMMENT,
									COLUMN_DEFAULT
                             FROM ` + sourceDatabase + `.INFORMATION_SCHEMA.COLUMNS
                            WHERE TABLE_CATALOG = '` + sourceDatabase + `'
                              AND TABLE_SCHEMA = '` + sourceSchema + `'
                              //AND TABLE_NAME LIKE 'ERP%'`
							  ;

    var columns = snowflake.createStatement({sqlText: listColumnsQuery}).execute();

	// 테이블 초기화
	var deleteQuery = `DELETE FROM ` + targetDatabase + `.` + targetSchema + `.` + targetTable + `
                          `;
	var deleteStatement = snowflake.createStatement({sqlText: deleteQuery});
    deleteStatement.execute();

    // 데이터 적재
    while (columns.next()) {
		var valueDatabaseName = columns.getColumnValue(1);
		var valueSchemaName = columns.getColumnValue(2);
        var valueTableName = columns.getColumnValue(3);
		var valueColumnName = columns.getColumnValue(4);
		var valueDataType = columns.getColumnValue(5);
		var valueIsNullable = columns.getColumnValue(6);
		var valueCharacterMaximumLength = columns.getColumnValue(7);
		var valueNumericPrecision = columns.getColumnValue(8);
		var valueNumericScale = columns.getColumnValue(9);
		var valueOrdinalPosition = columns.getColumnValue(10);
		var valueComment = columns.getColumnValue(11);
		var valueColumnDefault = columns.getColumnValue(12);
		
        var insertQuery = `INSERT INTO ` + targetDatabase + `.` + targetSchema + `.` + targetTable + ` (
								DATABASE_NAME,
								SCHEMA_NAME,
								TABLE_NAME,
								COLUMN_NAME,
								ORDINAL_POSITION,
								COLUMN_DEFAULT,
								DATA_TYPE,
								IS_NULLABLE,
								CHARACTER_MAXIMUM_LENGTH,
								NUMERIC_PRECISION,
								NUMERIC_SCALE,
								COMMENT,
								SAMPLE_VALUES,
								CREATE_TIMESTAMP
							)
							SELECT  '` + valueDatabaseName + `' AS DATABASE_NAME,
									'` + valueSchemaName + `' AS SCHEMA_NAME,
									'` + valueTableName + `' AS TABLE_NAME,
									'` + valueColumnName + `' AS COLUMN_NAME,
									` + valueOrdinalPosition + ` AS ORDINAL_POSITION,
									NULLIF('` + valueColumnDefault + `','null') AS COLUMN_DEFAULT,
									'` + valueDataType + `' AS DATA_TYPE,
									'` + valueIsNullable + `' AS IS_NULLABLE,
									` + valueCharacterMaximumLength + ` AS CHARACTER_MAXIMUM_LENGTH,
									` + valueNumericPrecision + ` AS NUMERIC_PRECISION,
									` + valueNumericScale + ` AS NUMERIC_SCALE,
									NULLIF('` + valueComment + `','null') AS COMMENT,
								(
									SELECT LISTAGG(TO_VARCHAR(` + valueColumnName + `), ', ')
									  FROM (
										SELECT DISTINCT ` + valueColumnName + `
										  FROM ` + valueDatabaseName + `.` + valueSchemaName + `.` + valueTableName + `
										 WHERE ` + valueColumnName + ` IS NOT NULL
										 ORDER BY RANDOM()
										 LIMIT ` + valueCount + `
									)
								) AS SAMPLE_VALUES,
									CURRENT_TIMESTAMP
                          `;

        try {
            var stmt = snowflake.createStatement({sqlText: insertQuery});

            var result = stmt.execute();
            result.next();
            var insertCount = result.getColumnValue(1);
            totalInsert += insertCount;

        } catch (err) {

            continue;

        }
    }

    return `총 ` + totalInsert + `개의 레코드가 적재되었습니다.`;
$$;