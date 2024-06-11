import numpy as np
import pandas as pd
import warnings

warnings.filterwarnings('ignore')

feat=['score','nbki_Days_since_last_credit', 'Nb_active_microcredits','Nb_active_mortgages', 'scoreRetailPersonal']
scale=[0.126310, 259.617877, 5.895513, 0.575404, 228.219860]
mean=[0.470280,120.505196, 1.705261, -0.248133, 172.331276]
coef=[ 0.812147,-0.058451, 0.429396, -0.078959, -0.762492]

mean_dict=dict(zip(feat, mean))
scale_dict=dict(zip(feat, scale))
coef_dict=dict(zip(feat, coef))

def logreg(data: pd.DataFrame)-> pd.Series:
    for i in feat:
        data[i] = ((data[[i]] - mean_dict[i]) / scale_dict[i])*coef_dict[i]
    data['PD'] =1- (1 / ( 1 + np.exp(-0.58302665 + data['score'] + data['nbki_Days_since_last_credit'] + data['Nb_active_microcredits'] + data['Nb_active_mortgages'] + data['scoreRetailPersonal'])))
    return data['PD']
