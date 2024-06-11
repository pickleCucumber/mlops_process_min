import joblib
import pandas as pd
import os
#from logreg import logreg
import time
from sqlalchemy import create_engine, text
import warnings
warnings.filterwarnings('ignore')
start=time.time()

# cut-off
tresh=0.84
# подгружаем модельку
BOOSTING_MODEL_PATH = os.path.join('', '', 'model', 'repeated_catboost.pkl')
boosting = joblib.load(BOOSTING_MODEL_PATH)


# коннектор к бд

conn = "mssql+pyodbc://login:password@lokalhost?driver=SQL+Server"
engine = create_engine(conn)

# запрос чек новых айди
query_check="""select distinct id from G"""

# добавить джоин на клиентскую таблу

#запрос сбора данных
request_path = os.path.join('', '', 'data', 'data.sql')
fd = open(request_path, 'r', encoding='utf-8')
query_data = fd.read()



# чтение данных(ну айдишник передать еще)
def read() ->pd.DataFrame:
    with  engine.connect() as conn:
        data = pd.read_sql_query(query_data, conn)
    return data

# инсерт typeid надо зарегать еще
def insert(data: pd.DataFrame):
    cursor = conn.cursor()
    for index, row in data.iterrows():
         cursor.execute("""insert into (appId, typeid, probability, threshold, trustML) 
                        values(?,?,?,?,?)""",
                        row.appId,row.typeid,row.probability,row.threshold,row.trustML)

    conn.commit()
    cursor.close()
    conn.close()


# cut-off
tresh=0.84
# подгружаем модельку
BOOSTING_MODEL_PATH = os.path.join('..', 'console', 'models', 'repeated_catboost.pkl')
boosting = joblib.load(BOOSTING_MODEL_PATH)
CALIBRATED_BOOSTING_MODEL_PATH = os.path.join('..',  'console', 'models', 'logreg.pkl')
logreg = joblib.load(CALIBRATED_BOOSTING_MODEL_PATH)

def model(data: pd.DataFrame) ->pd.DataFrame:
    # прогоняем бустинг
    data[['IdChannel', 'max_delay', 'region', 'Sex', 'Age', 'Income', 'num_order', 'Loan_amount', 'amountordersr', 'PDN', 'insurance', 'costs', 'cost_on_la']]=data[['IdChannel', 'max_delay', 'region', 'Sex', 'Age', 'Income', 'num_order', 'Loan_amount', 'amountordersr', 'PDN', 'insurance', 'costs', 'cost_on_la']].astype(float)
    data['score'] = boosting.predict_proba(data[['IdChannel', 'max_delay', 'region', 'Sex', 'Age', 'Income', 'num_order', 'Loan_amount', 'amountordersr', 'PDN', 'insurance', 'costs', 'cost_on_la']])[:, 1]
    data['PD'] = logreg.predict_proba(data[['score', 'nbki_Total_overdue_amount', 'nbki_Total_active_accounts',
                                            'nbki_Days_since_last_credit', 'Nb_delays_60_90_ever', 'Max_overdue',
                                            'Nb_active_consumer_credit', 'Nb_active_microcredits',
                                            'Nb_active_mortgages', 'scoreRetailPersonal']])[:, 1]

    # получаем скор логрега
    #data['PD']=logreg(data)
    return data

#типа список айдишников заявки с которыми сравнивать будем(за месяц или тип того)
apps=[]
while True:
    try:
        with engine.connect() as conn:
            appid=pd.read_sql_query(query_check, conn)
            # проверка новой заявки в списке
            app= [x for x in appid if x not in apps]
            if not app:
                continue
            else:
                data=read()
                data=model()
                insert(data)
    except Exception as e:
        print("\nAn error occurred: {0}.".format(str(e)))

    finally:
        conn.close()
