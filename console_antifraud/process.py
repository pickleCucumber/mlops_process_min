import pandas as pd
import os
import joblib
import category_encoders as ce
from user_agents import parse


##########################препроцессинг##########################
#энкодер
# encoder_path = os.path.join('C:\\Users\\Test_app\\Desktop\\console_antifraud', 'models', 'antifraud_encoder.pkl')
# encoder = joblib.load(encoder_path)

# def encoding(data: pd.DataFrame) -> pd.DataFrame:
#     # категориальные столбцы
#     enc = data[['device', 'device_model', 'Browser', 'OS']]
#
#     # соединяем
#     enc1 = encoder.transform(enc)
#     df = pd.concat([enc1, data[['Age', 'Passport_region', 'Loan_amount', 'Income', 'ip_first_oktet',  'ip_three_oktet', 'ip_second_oktet',
#            'ip_four_oktet', 'browser_version_first_oktet', 'browser_version__second_oktet', 'browser_version__three_oktet']]], axis=1)
#
#     return df

# парсинг ip адресов
def IP_parse(data: pd.DataFrame) -> pd.DataFrame:
    # with pd.option_context("future.no_silent_downcasting", True):

    data.IP_address = data.IP_address.fillna(0)
    data.IP_address = data.IP_address.astype(str)

    data['IP_address'] = [i.split(',')[0] if len(str(i)) > 14 else 0 for i in data.IP_address]
    data.IP_address = data.IP_address.astype(str)

    # октеты
    data['ip_first_oktet'] = [str(i).split('.')[0] if len(str(i).split('.')) != 0 else 0 for i in data.IP_address]
    data['ip_second_oktet'] = [str(i).split('.')[1] if len(str(i).split('.')) > 1 else 0 for i in data.IP_address]
    data['ip_three_oktet'] = [str(i).split('.')[2] if len(str(i).split('.')) > 2 else 0 for i in data.IP_address]
    data['ip_four_oktet'] = [str(i).split('.')[3] if len(str(i).split('.')) > 3 else 0 for i in data.IP_address]

    return data

# парсинг девайса, оси, браузера
def Device_Parse(data: pd.DataFrame) -> pd.DataFrame:

    data['DeviceFingerprint'] = data['DeviceFingerprint'].astype(str)
    us = parse(data.DeviceFingerprint[0])
    data['Browser'] = us.browser.family
    data['Browser_version'] = us.browser.version_string
    data['OS'] = us.os.family
    data['OS_version'] = us.os.version_string
    data['device'] = us.device.brand
    data['device_model'] = us.device.model
    return data
    
# парсинг браузера
def Browser_parse(data: pd.DataFrame) -> pd.DataFrame:

    data.Browser_version = data.Browser_version.astype(str)
    data['browser_version_first_oktet'] = [i.split('.')[0] if len(i.split('.')) > 1 else 0 for i in data.Browser_version]
    data['browser_version__second_oktet'] = [i.split('.')[1] if len(i.split('.')) > 1 else 0 for i in data.Browser_version]
    data['browser_version__three_oktet'] = [i.split('.')[2] if len(i.split('.')) == 3 else 0 for i in data.Browser_version]

    return data


# пайплайн
def preprocess_pipeline(data: pd.DataFrame) -> pd.DataFrame:
    with pd.option_context("future.no_silent_downcasting", True):
        data = data.infer_objects(copy=False).fillna(0)


    data['Work_type']=data['Work_type'].max()
    data['Loan_amount']=data['Loan_amount'].max()
    data['Organization']=data['Organization'].max()
    data['Income']=data['Income'].max()
    data['IP_address']=data['IP_address'].max()
    data['FMS_check']=data['FMS_check'].max()
    data['Marital_status']=data['Marital_status'].max()
    data['Organization_category']=data['Organization_category'].max()
    data['Passport_maturity']=data['Passport_maturity'].max()
    data['Age']=data['Age'].max()
    data['Reg_region']=data['Reg_region'].max()
    data['Liv_region']=data['Liv_region'].max()
    data['Average_Income']=data['Average_Income'].max()
    data['Match_phone']=data['Match_phone'].max()
    data1=data.iloc[:1]
    data1 = IP_parse(data1)

    return data1



####################################для записи результата##############################################################

res=pd.DataFrame(columns=['appId', 'typeid', 'probability', 'threshold', 'trustML'])
def postprocess(app: str, tresh: float, X: pd.Series, typeid=9) -> pd.DataFrame:
    res['appId']=app
    res['typeid']=typeid
    res['probability']=round(X, 5).astype(float)
    res['threshold']=tresh
    #вот тут спорно
    res['trustML']=1
    return res

