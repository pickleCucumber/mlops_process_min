import joblib
import pandas as pd
import pyodbc
import datetime
from datetime import datetime as dt
import os
from process import *
import time
import datetime
from sqlalchemy import create_engine, text
import pyodbc
import gc

import warnings
warnings.filterwarnings('ignore')
tresh=0.8323209484322648
# подгружаем модельку

BOOSTING_MODEL_PATH = os.path.join('D:\\GITREPO\\console_test\\console_antifraud', 'models', 'antifraud_catboost.pkl')
boosting = joblib.load(BOOSTING_MODEL_PATH)

# используемые признаки

col = ['ip_second_oktet', 'ip_three_oktet', 'ip_four_oktet', 'Work_type', 'Match_phone', 'Marital_status', 'FMS_check', 'Liv_region', 'Income', 'Organization_category', 'Passport_maturity', 'Age', 'Loan_amount', 'Average_Income', 'Reg_region', 'Organization']




# коннектор к бд
conn = "mssql+pyodbc://log:psw@localhost/DMS?driver=SQL+Server"
engine = create_engine(conn)
conn2 = pyodbc.connect('DRIVER={SQL Server}; SERVER=localhost; DATABASE=db; UID=log; PWD=psw')
cursor2 = conn2.cursor()


# запрос чек новых айди
query0="""select distinct AppId from [ServiceRequest_Queue] with(nolock) where CounterpartyId = 29 and Status = 0 and cast(dtInsert as date)=cast(GETDATE() as date)"""
# StatusProcessTypeId = 3 - требуется запрос внешнего сервиса; CounterpartyId = 29 - внешний сервис - ML модель




def model(data: pd.DataFrame) -> pd.DataFrame:
    #препроцессинг
    X = preprocess_pipeline(data)

    # прогоняем бустинг
    X['score'] = boosting.predict_proba(X[col])[:, 1]

    return X


while True:
    try:
        conn = pyodbc.connect('DRIVER={SQL Server}; SERVER=localhost; DATABASE=db; UID=log; PWD=psw')
        cursor = conn.cursor()

        app = pd.read_sql_query(query0, conn)
        apps = app['AppId'].to_list()
        if len(apps) != 0:
            print('Найдена новая заявка')

            print('app', apps)
            print(str(datetime.datetime.now()))
            cursor.execute("""update [ServiceRequest_Queue] set Status = 1, dtSendRequest=getdate() where AppId=""" + str(apps[0]) +""" and  CounterpartyId = 29 """)
            conn.commit()
            data = pd.read_sql_query("""
выгрузка данных
where app.id=""" + str(apps[0]) + """
          """, conn)

            X = model(data)
            res = postprocess(app=str(apps[0]), tresh=tresh, X=X['score'])

            print(res)
            gc.collect()
            conn2 = pyodbc.connect('DRIVER={SQL Server}; SERVER=localhost; DATABASE=db; UID=log; PWD=psw')
            cursor2 = conn2.cursor()

            for index, row in res.iterrows():
                try:
                    cursor2.execute("""insert into dms.dbo.output_vector_ml (appId, typeid, probability, threshold, trustML)
                                                values(?,?,?,?,?)""",
                                    row.appId, row.typeid, row.probability, row.threshold, row.trustML)

                    conn2.commit()
                except Exception as e:
                    continue

            print('инсерт', datetime.datetime.now().isoformat("#"))
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






