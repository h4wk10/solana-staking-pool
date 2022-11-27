import streamlit as st
import pandas as pd
import numpy as np
import plotly.figure_factory as ff
import plotly.express as px
from shroomdk import ShroomDK
from datetime import datetime

sdk = ShroomDK(st.secrets['sdk_key'])

# SETTING PAGE CONFIG TO WIDE MODE AND ADDING A TITLE AND FAVICON
st.set_page_config(layout="wide", page_title="Solana Staking Pool", page_icon="☀️")
st.title('Solana Staking Pool - Live Dashboard')

def update_data():

  st.text('Updating Staking Pool data ...')
  sql_1 = f'''
  select *
  from solana.core.fact_stake_pool_actions
  order by block_timestamp'''

  fact_stake_pool_actions = pd.DataFrame()
  for i in range (1,10):
    # print("Loading Page number", i)
    result = sdk.query(sql_1, page_number=i)
    result = pd.DataFrame(result.records)
    fact_stake_pool_actions = pd.concat([fact_stake_pool_actions, result])

  fact_stake_pool_actions = fact_stake_pool_actions.reset_index(drop = True)
  fact_stake_pool_actions.to_csv('data/fact_stake_pool_actions.csv', index = False)
  st.text('Staking Pool data is up to date!')

def load_data():
    df = pd.read_csv(
        "data/fact_stake_pool_actions.csv"
    )
    return df

def dropdown(df): # Dropdown
  df.stake_pool_name = df.stake_pool_name.str.title()
  option = st.selectbox(
      'Select Staking Pool',
      df['stake_pool_name'].unique())
  st.write('You selected:', option)
  return option

def update_button_callback():
  duration = 0
  # if st.button('Update Data'):
  st.write('Latest data is', max(df.block_timestamp)) 
  latest = datetime.strptime(max(df.block_timestamp)[:-4], '%Y-%m-%d %H:%M:%S')
  duration = np.round((datetime.now() - latest).total_seconds() /3600, 2)
  st.write('Updated', duration, 'hours ago')

  if duration > st.secrets['update_delay']:
    st.text('Fetching new data. This may take a while ...')
    update_data()
  else:
    st.text('Data is up to date! There is no need to fetch.')


df = load_data()

dropdown(df) 
st.button('Update Data' , on_click = update_button_callback, help="Check for the latest database update in the last 24 hours") 




