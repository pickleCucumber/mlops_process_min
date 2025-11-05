import joblib
import pandas as pd
import datetime
import logging
import sys
from datetime import datetime as dt
from process import *
import time
import datetime
from sqlalchemy import create_engine, text
import pyodbc
import gc
from dotenv import load_dotenv
import os
import warnings
warnings.filterwarnings('ignore')
tresh=0.8323209484322648

load_dotenv()
host = os.getenv("db_host")
psw = os.getenv("db_pass")
log = os.getenv("db_log")
###логирование
LOG_DIR = r"C:\Users\Test_app\Desktop\logger"
if len(sys.argv) > 1:
    log_id = sys.argv[1]
else:
    log_id = "antifraud"

LOG_FILENAME = f"{log_id}_monitor.log"

FULL_LOG_PATH = os.path.join(LOG_DIR, LOG_FILENAME)
os.makedirs(LOG_DIR, exist_ok=True)
logger = logging.getLogger('AntiFraudLogger')
logger.setLevel(logging.INFO)
#обработчик для записи в файл
file_handler = logging.FileHandler(FULL_LOG_PATH, mode='a', encoding='utf-8')
file_handler.setLevel(logging.INFO)
#обработчик для вывода в консоль
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

logger.info("==========================================================")
logger.info(f"Скрипт мониторинга запущен. Лог: {FULL_LOG_PATH}")
logger.info("==========================================================")
# ------------------------------------------------------------------------

# подгружаем модельку
BOOSTING_MODEL_PATH = os.path.join('D:\\GITREPO\\console_test\\console_antifraud', 'models', 'antifraud_catboost.pkl')
boosting = joblib.load(BOOSTING_MODEL_PATH)

# используемые признаки

col = ['ip_second_oktet', 'ip_three_oktet', 'ip_four_oktet', 'Work_type', 'Match_phone', 'Marital_status', 'FMS_check', 'Liv_region', 'Income', 'Organization_category', 'Passport_maturity', 'Age', 'Loan_amount', 'Average_Income', 'Reg_region', 'Organization']




# коннектор к бд
conn = f"mssql+pyodbc://{log}:{psw}@{host}/DMS?driver=SQL+Server"
engine = create_engine(conn)
conn2 = pyodbc.connect(r"DRIVER={SQL Server}; "
                        f"SERVER={host}; DATABASE=DMS; UID={log}; PWD={psw}")
cursor2 = conn2.cursor()


# запрос чек новых айди
query0="""select distinct AppId from Queue with(nolock) where CounterpartyId = 29 and Status = 0 and cast(dtInsert as date)=cast(GETDATE() as date)"""
# StatusProcessTypeId = 3 - требуется запрос внешнего сервиса; CounterpartyId = 29 - внешний сервис - ML модель




def model(data: pd.DataFrame) -> pd.DataFrame:
    #препроцессинг
    X = preprocess_pipeline(data)

    # прогоняем бустинг
    X['score'] = boosting.predict_proba(X[col])[:, 1]

    return X


while True:
    try:
        conn = pyodbc.connect(r"DRIVER={SQL Server}; "
                               f"SERVER={host}; DATABASE=DMS; UID={log}; PWD={psw}")
        cursor = conn.cursor()

        app = pd.read_sql_query(query0, conn)
        apps = app['AppId'].to_list()
        if len(apps) != 0:
            logger.info(
                f"[{dt.now().isoformat(' ', 'seconds')}] Найдена новая заявка: {apps} в {log_id}")

            cursor.execute("""update Queue set Status = 1, dtSendRequest=getdate() where AppId=""" + str(apps[0]) +""" and  CounterpartyId = 29 """)
            conn.commit()
            logger.info(f"Статус заявки {apps} обновлен до 'в обработке'.")
            data = pd.read_sql_query("""
select * from vector
where app.id=""" + str(apps[0]) + """
          """, conn)

            X = model(data)
            res = postprocess(app=str(apps[0]), tresh=tresh, X=X['score'])
            logger.info(f"Результат скоринга для {apps}: {res['probability'].iloc[0]:.4f} (Треш: {tresh})")
            #print(res)
            gc.collect()
            conn2 = pyodbc.connect(r"DRIVER={SQL Server}; "
                                   f"SERVER={host}; DATABASE=DMS; UID={log}; PWD={psw}")
            cursor2 = conn2.cursor()

            for index, row in res.iterrows():
                try:
                    cursor2.execute("""insert into output_vector_ml (appId, typeid, probability, threshold, trustML)
                                                values(?,?,?,?,?)""",
                                    row.appId, row.typeid, row.probability, row.threshold, row.trustML)

                    conn2.commit()
                except Exception as e:
                    logger.error(f"Ошибка при вставке результата для {apps}: {e}",
                                 exc_info=False)
                    continue

            logger.info(f"Результаты для {apps} занесены в БД. [{dt.now().isoformat(' ', 'seconds')}]")
            #print('инсерт', datetime.datetime.now().isoformat("#"))
            cursor2.close()
            conn2.close()
            apps.pop(0)

            del data
            del res
            gc.collect()
    except Exception as e:
        logger.error(f"Опять правки в бд: {e}", exc_info=True)
        #print("\nТы ебан: {0}.".format(str(e)))
        time.sleep(60)
        sys.exit(1)
    finally:
        conn.close()






