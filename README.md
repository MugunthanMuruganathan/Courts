#-- In python kernel

import pandas as pd
from teradataml import copy_to_sql

df = pd.read_csv("stevens_training.csv", header=1)
df.columns = ["Docket","Term","Circuit","Issue","Petitioner","Respondent","LowerCourt","Unconst","Reverse"]


#%pip install -q teradataml==17.20.0.6 teradatamodelops==7.0.3 matplotlib==3.8.2

from teradataml import *
import os
import getpass
import logging
import sys




%run -i ../UseCases/startup.ipynb
eng = create_context(host = 'host.docker.internal', username='demo_user', password = password)
print(eng)


copy_to_sql(df = df, table_name = "stevens", index=True, index_label="Docketid", if_exists="replace")


#-- In Teradata SQL


%connect local, hidewarnings=true 

```sql
CREATE TABLE STEVENS_FEATURES AS 
    (SELECT 
        Docketid,
		docket,
		Term,
		Circuit,
		Issue,
		Petitioner,
		Respondent,
		LowerCourt,
		Unconst
    FROM stevens 
    ) WITH DATA;
    

CREATE TABLE STEVENS_DIAGNOSIS AS 
    (SELECT 
        DocketId,
        Reverse
    FROM stevens 
    ) WITH DATA;
	

