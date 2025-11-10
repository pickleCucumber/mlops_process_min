import joblib
import pandas as pd
import pyodbc
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
#from memory_profiler import profile

import warnings
warnings.filterwarnings('ignore')
load_dotenv()
host = os.getenv("db_host")
psw = os.getenv("db_pass")
log = os.getenv("db_log")
###логирование
LOG_DIR = r"C:\Users\Test_app\Desktop\logger"
if len(sys.argv) > 1:
    log_id = sys.argv[1]
else:
    log_id = "repeated"

LOG_FILENAME = f"{log_id}_monitor.log"

FULL_LOG_PATH = os.path.join(LOG_DIR, LOG_FILENAME)
os.makedirs(LOG_DIR, exist_ok=True)
logger = logging.getLogger('RepeatedLogger')
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

tresh=0.84
# подгружаем модельку
BOOSTING_MODEL_PATH = os.path.join('D:\GITREPO\\console', 'models', 'repeated_catboost.pkl')
boosting = joblib.load(BOOSTING_MODEL_PATH)
CALIBRATED_BOOSTING_MODEL_PATH = os.path.join('D:\\GITREPO\\console_test\\console', 'models', 'logreg.pkl')
logreg = joblib.load(CALIBRATED_BOOSTING_MODEL_PATH)


conn = f"mssql+pyodbc://{log}:{psw}@{host}/DMS?driver=SQL+Server"
engine = create_engine(conn)
conn2 = pyodbc.connect(r"DRIVER={SQL Server}; "
                        f"SERVER={host}; DATABASE=DMS; UID={log}; PWD={psw}")
cursor2 = conn2.cursor()


# запрос чек новых айди
query0="""select distinct AppId from [ServiceRequest_Queue] with(nolock) where CounterpartyId = 27 and Status = 0 and cast(dtInsert as date)=cast(GETDATE() as date)"""
# StatusProcessTypeId = 3 - требуется запрос внешнего сервиса; CounterpartyId = 23 - внешний сервис - ML модель

def insert(res: pd.DataFrame):
    cursor2=conn2.cursor()
    for index, row in res.iterrows():
         cursor2.execute("""insert into dms.dbo.output_vector_ml (appId, typeid, probability, threshold, trustML) 
                        values(?,?,?,?,?)""",
                        row.appId,row.typeid,row.probability,row.threshold,row.trustML)

    conn2.commit()
    cursor2.close()
    conn2.close()


def model(data: pd.DataFrame) ->pd.DataFrame:
    #препроцессинг
    data[col_boost]=preprocess(data[col_boost])
    # прогоняем бустинг
    data['score'] = boosting.predict_proba(data[col_boost])[:, 1]
    #препроцессинг
    data[col_logreg] = preprocess(data[col_logreg])
    # вызываем логрег
    data['PD'] = logreg.predict_proba(data[col_logreg])[:, 1]


    return data


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

            # try:
            cursor.execute("""update [ServiceRequest_Queue] set Status = 1, dtSendRequest=getdate() where AppId=""" + str(apps[0]) +""" and  CounterpartyId = 27 """)
            conn.commit()

            logger.info(f"Статус заявки {apps} обновлен до 'в обработке'.")

            data = pd.read_sql_query("""
select * from vector
where app.id = """ + str(apps[0]) + """ """, conn)


            data = model(data)
            res = postprocess(app=str(apps[0]), tresh=tresh, X=data['PD'])

            logger.info(f"Результат скоринга для {apps}: {res['probability'].iloc[0]:.4f} (Треш: {tresh})")

            gc.collect()
            conn2 = pyodbc.connect(r"DRIVER={SQL Server}; "
                            f"SERVER={host}; DATABASE=DMS; UID={log}; PWD={psw}")
            cursor2 = conn2.cursor()

            for index, row in res.iterrows():
                try:
                    cursor2.execute("""insert into dms.dbo.output_vector_ml (appId, typeid, probability, threshold, trustML)
                                                values(?,?,?,?,?)""",
                                    row.appId, row.typeid, row.probability, row.threshold, row.trustML)

                    conn2.commit()
                except Exception as e:
                    logger.error(f"Ошибка при вставке результата для {apps}: {e}",
                                 exc_info=False)
                    continue

            logger.info(f"Результаты для {apps} занесены в БД. [{dt.now().isoformat(' ', 'seconds')}]")

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


