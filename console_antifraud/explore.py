import joblib
import pandas as pd
import datetime
import logging
import sys
from datetime import datetime as dt
import os
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
LOG_FILENAME = "antifraud_monitor.log"
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
        conn = pyodbc.connect(r"DRIVER={SQL Server}; "
                               f"SERVER={host}; DATABASE=DMS; UID={log}; PWD={psw}")
        cursor = conn.cursor()

        app = pd.read_sql_query(query0, conn)
        apps = app['AppId'].to_list()
        if len(apps) != 0:
            logger.info(
                f"[{dt.now().isoformat(' ', 'seconds')}] Найдена новая заявка: {apps}")
            #print('app', apps)
            #print(str(datetime.datetime.now()))
            cursor.execute("""update [ServiceRequest_Queue] set Status = 1, dtSendRequest=getdate() where AppId=""" + str(apps[0]) +""" and  CounterpartyId = 29 """)
            conn.commit()
            logger.info(f"Статус заявки {apps} обновлен до 'в обработке'.")
            data = pd.read_sql_query("""
select distinct workTypeId as Work_type, (isnull(app.AmountPurchaseOriginal,0.00)-isnull(app.InitialFee,0.00)) as Loan_amount, 
app.OrganizationId as Organization, AverageMonthlyIncome as Income,
(case when apps.IP_Service IS NOT NULL then apps.IP_Service else auth.lastIP end) IP_address, 
(case when fms.Id is not null then fms.FMS_check else 2 end) FMS_check, cl.martialId as Marital_status,  
org1.CategoryGoodsId as Organization_category,
DATEDIFF(MONTH,cast(ipass.IssueDate as date),cast(app.dtInput as date)) Passport_maturity, 
(case when cl.BirthDay IS NOT NULL and app.dtInput IS NOT NULL then cast(YEAR(app.dtInput-cast(cl.BirthDay as datetime)) - 1900 as int) else NULL end) Age,
(case when addr.AddressTypeId IN (1) and isnumeric(addr.kladrCode)=1 then cast(substring(cast(addr.kladrCode as nvarchar),1,2) as int) else NULL end) Reg_region,
(case when addr.AddressTypeId IN (2) and isnumeric(addr.kladrCode)=1 then cast(substring(cast(addr.kladrCode as nvarchar),1,2) as int) else NULL end) Liv_region,
Average_Income= (select dms.dbo.GetAverageIncome(""" + str(apps[0]) + """)), Mobile_phone=auth.MobilePhone,
ph.Match_phone
from Billing.dbo.Applications as app
left join Billing.dbo.Client cl on app.ClientId=cl.id
left join Billing.dbo.Client_Work cw with(nolock) on app.clientid=cw.ClientId
left join Billing.dbo.Client_AdditionalInfo ad on  app.clientid=ad.ClientId
left join Billing.dbo.Organization as org1 with(nolock) on app.OrganizationId IN (org1.id)
left join ( --- проверено, время работы <1 сек 
select app.Id as AppId, 
app.ClientId,
(case when bscc.Id IS NOT NULL then cast(bscc.ip as nvarchar) else NULL end) as IP_Service,
app.OrganizationId as OrganizationId
from Billing.dbo.Applications as app with(nolock)
Left Outer Join BillingService.dbo.ServiceApplication as bsapp with(nolock) on bsapp.Billing_AppId IN (app.Id)
Left Outer Join BillingService.dbo.ClientConsentDetails as bscc with(nolock) on bscc.Service_AppId IN (bsapp.Id)
where app.Id IN (""" + str(apps[0]) + """)) as apps on app.id=apps.AppId
left join (  --- проверено, время работы <1 сек 
select  Billing.dbo.CleaningPhone(auth.MobilePhone) as MobilePhone, lastIP, 
auth.ClientId as ClientId  
from Billing.dbo.[Authorization] auth with(nolock) ) as auth on app.clientid=auth.clientid
left join ( --- проверено, время работы <1 сек
select ClientId, regionName, AddressTypeId, kladrCode 
from Billing.dbo.[Address] with(nolock) 
where AddressTypeId IN (1,2,5)
) addr on app.ClientId=addr.ClientId
left join Billing.dbo.Passport as ipass on app.ClientId=ipass.ClientId
Left Outer Join DMS.dbo.Check_fms as fms with(nolock) on fms.AppId IN (""" + str(apps[0]) + """)
left join (select ClientId, count(distinct(ClientId)) Match_phone from dms.dbo.Client_match with(nolock) where 
(  
(MobilePhone IS NOT NULL and MobilePhone=MobilePhone) or 
(HomePhone IS NOT NULL and HomePhone=MobilePhone) or 
(WorkPhone IS NOT NULL and WorkPhone=MobilePhone) or 
(ExtPhone IS NOT NULL and ExtPhone=MobilePhone)
) group by ClientId) ph on app.ClientId=ph.clientid
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
                    cursor2.execute("""insert into dms.dbo.output_vector_ml (appId, typeid, probability, threshold, trustML)
                                                values(?,?,?,?,?)""",
                                    row.appId, row.typeid, row.probability, row.threshold, row.trustML)

                    conn2.commit()
                except Exception as e:
                    logger.error(f"Ошибка при вставке результата для {apps}: {e}",
                                 exc_info=False)  # Не нужно выводить полный traceback на каждую ошибку вставки
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
    finally:
        conn.close()






import joblib
import pandas as pd
import datetime
import logging
import sys
from datetime import datetime as dt
import os
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
LOG_FILENAME = "antifraud_monitor.log"
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
                f"[{dt.now().isoformat(' ', 'seconds')}] Найдена новая заявка: {apps}")
            #print('app', apps)
            #print(str(datetime.datetime.now()))
            cursor.execute("""update [ServiceRequest_Queue] set Status = 1, dtSendRequest=getdate() where AppId=""" + str(apps[0]) +""" and  CounterpartyId = 29 """)
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
                    cursor2.execute("""insert into dms.dbo.output_vector_ml (appId, typeid, probability, threshold, trustML)
                                                values(?,?,?,?,?)""",
                                    row.appId, row.typeid, row.probability, row.threshold, row.trustML)

                    conn2.commit()
                except Exception as e:
                    logger.error(f"Ошибка при вставке результата для {apps}: {e}",
                                 exc_info=False)  # Не нужно выводить полный traceback на каждую ошибку вставки
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
    finally:
        conn.close()






