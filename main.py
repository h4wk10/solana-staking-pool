import streamlit as st
import pandas as pd
import numpy as np
import plotly.figure_factory as ff
from shroomdk import ShroomDK

sdk = ShroomDK(st.secrets['sdk_key'])

# SETTING PAGE CONFIG TO WIDE MODE AND ADDING A TITLE AND FAVICON
st.set_page_config(layout="wide", page_title="Solana Staking Pool", page_icon="☀️")
st.title('Solana Staking Pool - Live Dashboard')

sql_1 = f'''select 
distinct initcap(stake_pool_name) as pool_name
from solana.core.fact_stake_pool_actions
order by pool_name
'''

result = sdk.query(sql_1)
result = pd.DataFrame(result.records)

option = st.selectbox(
    'Select Staking Pool',
    result.pool_name)

st.write('You selected:', option)

sql_2 = f'''
select 
block_timestamp::date as date,
    tx_id,
  address as staker,
  amount/1e9 as amount_sol,
  initcap(stake_pool_name) as Pool
  
from solana.core.fact_stake_pool_actions
  where succeeded = 'TRUE' and stake_pool_name ilike '{option}'
'''

result2 = sdk.query(sql_2)
result2 = pd.DataFrame(result2.records)

st.dataframe(result2)