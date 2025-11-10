import pandas as pd

# используемые признаки
col_boost=['IdChannel', 'max_delay', 'region', 'Sex', 'Age', 'Income', 'num_order', 'Loan_amount', 'amountordersr', 'PDN', 'insurance', 'costs', 'cost_on_la']
col_logreg=['score', 'Nb_active_microcredits', 'nbki_Days_since_last_credit', 'scoreRetailPersonal', 'Nb_active_mortgages']

# для записи результата
res=pd.DataFrame(columns=['appId', 'typeid', 'probability', 'threshold', 'trustML'])
def preprocess(data: pd.DataFrame) -> pd.DataFrame:
    data=data.fillna(0)
    data=data.astype(float)
    return data

def postprocess(app: str, tresh: float, X: pd.Series, typeid=5) -> pd.DataFrame:
    res['appId']=app
    res['typeid']=typeid
    res['probability']=round(X, 5).astype(float)
    res['threshold']=tresh
    #вот тут спорно
    res['trustML']=1
    return res

