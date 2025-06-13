# 퍼시스 CRM 고도화 프로젝트

## 🗂️ 프로젝트 개요
퍼시스 CRM 시스템 고도화를 위한 데이터 파이프라인 및 마트 구축 프로젝트입니다.  
Kafka에서 전달된 데이터를 수신 및 변환하여 Snowflake에 적재하고, 스케줄 기반으로 자동화된 데이터 흐름을 구현했습니다.

## 🧩 사용 기술
- Kafka (데이터 수신)
- Python (변환 로직)
- Snowflake (데이터 적재 및 마트 구축)
- SQL, 프로시저, Task Graph

## 🧪 주요 업무
- Kafka에서 수신한 CRM 데이터를 가공하여 ODS/분석 테이블로 적재
- Snowflake 기반 데이터 모델 설계 및 저장 프로시저 작성
- Task Graph를 활용한 자동화 파이프라인 구성 및 모니터링

## ✅ 주요 성과
- 수동 적재 프로세스를 자동화하여 운영 효율 향상
- 분석 데이터 적재 구조 개선으로 대시보드 응답 속도 안정화

## 📁 구성 파일
- `README.md`: 데이터 흐름 및 설계 요약
- `pipeline 폴더`: 파이프라인 구축을 위한 프로시저, 테이블 및 작업 생성문과 Task Graph 구조도
- `call_loop_procedure.sql`: 파라미터를 매월 말일로 설정하여 프로시저를 호출하는 루프 프로시저
- `data_profiling_procedure.sql`: 데이터 프로파일링 결과 적재 프로시저
- `data_profiling_table_ddl.sql`: 데이터 프로파일링 결과 적재 테이블 생성문
- `get_procedure_ddl_procedure.sql`: 모든 저장 프로시저 생성문을 추출하는 프로시저 (업무기록용)

