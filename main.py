# install dependencies:
# uv add python-dotenv pymssql pymysql

from logger_kki import LoggerKKI
import os
from dotenv import load_dotenv
import pymssql
import pymysql


# kki_logger.py에서 logger 객체를 가져옵니다.
logger = LoggerKKI(logging_interval="Y").get_logger()

# .env 파일 로드
load_dotenv()

# MSSQL 접속 정보 (환경변수에서 불러오기)
mssql_config = {
    'server': os.getenv('MSSQL_SERVER'),
    'port': int(os.getenv('MSSQL_PORT', 1433)),  # 기본 포트: 1433
    'user': os.getenv('MSSQL_USER'),
    'password': os.getenv('MSSQL_PASSWORD'),
    'database': os.getenv('MSSQL_DATABASE'),
    'charset': 'cp949'  # <- Korean_Wansung_CI_AS 대응
}

# MySQL 접속 정보 (환경변수에서 불러오기)
mysql_config = {
    'host': os.getenv('MYSQL_HOST'),
    'port': int(os.getenv('MYSQL_PORT', 3306)),  # 기본 포트: 3306
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD'),
    'database': os.getenv('MYSQL_DATABASE'),
    'charset': 'utf8'
}

###############################################################################################################################
# MSSQL에서 데이터 조회
###############################################################################################################################
def fetch_from_mssql():
    conn = None
    cursor = None

    try:
        conn = pymssql.connect(**mssql_config)
        cursor = conn.cursor()
        query = (
            """
            SELECT h.h_jume
                , h.h_name
                , s.jumehuga
                , (ISNULL(h.h_limit, 0) - ISNULL(j.jm_misu, 0)) AS credit_amount
            FROM hando AS h
                , jumemisu as j
                , sajumecode as s
            WHERE h.h_jume = j.jm_code
                AND j.jm_code = s.jumecode
            """
        )
        cursor.execute(query)
        rows = cursor.fetchall()
        return rows

    except pymssql.DatabaseError as db_err:
        logger.error(f"MSSQL 데이터베이스 오류 발생: {db_err}")
        return []

    except Exception as e:
        logger.error(f"알 수 없는 오류 발생: {e}")
        return []

    finally:
        # 연결 종료
        if cursor:
            cursor.close()
        if conn:
            conn.close()

###############################################################################################################################
# MySQL에 데이터 삽입
###############################################################################################################################
def insert_into_mysql(rows):
    conn = None
    cursor = None

    try:
        conn = pymysql.connect(**mysql_config)
        cursor = conn.cursor()

        #######################################################################################################################
        # intermediate_wholesaler_credit 테이블 초기화
        #######################################################################################################################
        truncate_table_query = (
            """
            TRUNCATE TABLE intermediate_wholesaler_credit
            """
        )
        cursor.execute(truncate_table_query)
        conn.commit()

        #######################################################################################################################
        # intermediate_wholesaler_credit 데이터 삽입
        #######################################################################################################################
        insert_query = (
            """
            INSERT INTO intermediate_wholesaler_credit (h_jume, h_name, jumehuga, credit_amount) VALUES (%s, %s, %s, %s)
            """
        )
        # 여러 행을 한꺼번에 insert
        cursor.executemany(insert_query, rows)
        inserted_count = cursor.rowcount  # 삽입된 행의 수
        conn.commit()
        
        #######################################################################################################################
        # intermediate_wholesaler_credit 데이터 삽입 갯수 로깅
        #######################################################################################################################
        logger.info(f"Inserted Count : {inserted_count}")
        #######################################################################################################################

    except pymysql.DatabaseError as db_err:
        logger.error(f"MySQL 데이터베이스 오류 발생: {db_err}")
        conn.rollback()

    except Exception as e:
        logger.error(f"알 수 없는 오류 발생: {e}")
        conn.rollback()

    finally:
        # 연결 종료
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    data = fetch_from_mssql()

    # data가 비어 있지 않다면
    if data:
        insert_into_mysql(data)
    else:
        logger.warning("MSSQL에서 조회된 데이터가 없습니다. MySQL로 전송하지 않습니다.")