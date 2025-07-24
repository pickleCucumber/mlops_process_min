import joblib
import pandas as pd
import datetime
from datetime import datetime as dt
import os
from process import *
import requests

import time
import datetime
from sqlalchemy import create_engine, text
import pyodbc
import gc
import gc
from dotenv import load_dotenv
import warnings
warnings.filterwarnings('ignore')
# подгружаем модельку
load_dotenv()
host = os.getenv("db_host")
psw = os.getenv("db_pass")
log = os.getenv("db_log")
BOOSTING_MODEL_PATH = os.path.join('D:\\GITREPO\\console_test\\console_nerez', 'models', 'nerez_new_47_44.pkl')
boosting = joblib.load(BOOSTING_MODEL_PATH)

# используемые признаки
# для старой модели
col0=['dtstart',  'sex',  'birthday',  'citizenshipid',  'martialid',  'dependents',  'sitename',  'DOC',  'averagemonthlyincome',  'Days_since_last_credit',  'Max_overdue',
  'Nb_delays_90plus_ever_eq',  'CH_length_eq',  'S_hare_active_credit',  'Score',  'MatchingLevel',  'INTEGRALSCOREValueId',  'LIFETIMEBINValueId',  'requested_amount']
# для новой модели
col=['nation0','megafon_score','age','sex','LIFETIMEBINValueId','S_hare_active_credit','averagemonthlyincome',
'martialid','AmountPurchaseOriginal','InitialFee','BLOCKCNTValueId','CompanyTypeId','mail_score','Nb_delays_5_30_ever_eq','creditperiod','cnt_apps']


# запрос чек новых айди
query0="""select distinct AppId from [ServiceRequest_Queue] with(nolock) where CounterpartyId = 23 and Status = 0 and cast(dtInsert as date)=cast(GETDATE() as date)"""
# StatusProcessTypeId = 3 - требуется запрос внешнего сервиса; CounterpartyId = 29 - внешний сервис - ML модель




def old_model(data: pd.DataFrame) -> pd.DataFrame:
    url = "http://localhost:8081/serve/neres_lgbm"

    data['dtstart'] = data['dtstart'].astype(str)
    data['birthday'] = data['birthday'].astype(str)
    pay = {"row": data.iloc[0][col0].fillna(-1).to_dict()}
    response = requests.post(url, json=pay)
    res = response.json()
    df = pd.DataFrame([res])

    return df


while True:
    try:
        conn = pyodbc.connect(r"DRIVER={SQL Server}; "
                        f"SERVER={host}; DATABASE=DMS; UID={log}; PWD={psw}")
        cursor = conn.cursor()
        app = pd.read_sql_query(query0, conn)
        apps = app['AppId'].to_list()
        if len(apps) != 0:
            print('Найдена новая заявка')

            print('app', apps)
            print(str(datetime.datetime.now()))
            cursor.execute("""update [ServiceRequest_Queue] set Status = 1, dtSendRequest=getdate() where AppId=""" + str(apps[0]) +""" and  CounterpartyId = 23 """)
            conn.commit()
            data = pd.read_sql_query("""
SELECT vector
where app.id=""" + str(apps[0]) + """
          """, conn)
# старая модель
            X = old_model(data)
            res = postprocess(app=str(apps[0]), tresh=X['Threshold'], X=X['Probability'], typeid=2)
            print(res)

# сбор мусора и занесение в бд
           # gc.collect()
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
                    continue

            print('инсерт старой', datetime.datetime.now().isoformat("#"))

# новая модель
            data0 = preprocess_new(data)
            prob=boosting.predict_proba(data0[col])[:, 1][0]
            res1 = postprocess(app=str(apps[0]), tresh=0.6, X=prob, typeid=1)
            for index, row in res1.iterrows():
                try:
                    cursor2.execute("""insert into dms.dbo.output_vector_ml (appId, typeid, probability, threshold, trustML)
                                                values(?,?,?,?,?)""",
                                    row.appId, row.typeid, row.probability, row.threshold, row.trustML)

                    conn2.commit()
                except Exception as e:
                    continue

            print('инсерт новой модели', datetime.datetime.now().isoformat("#"))
            print(res1)
            cursor2.close()
            conn2.close()
            apps.pop(0)
            del data
            del res
            gc.collect()
    except Exception as e:
        print("\nТы ебан: {0}.".format(str(e)))

    finally:
        conn.close()






