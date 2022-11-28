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

# SETUP
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

def get_net_volume():
  actions = df["action"].isin(['deposit', 'deposit_stake', 'deposit_dao', 'deposit_dao_stake'])
  cols = ['stake_pool_name', 'tx_id', 'block_timestamp',
          'succeeded', 'action', 'amount']
  deposits = df.loc[actions, cols]

  deposits['amount'] = deposits['amount'].astype('float')/10**9
  deposits = deposits[(deposits.succeeded == True)]

  deposits['block_timestamp'] = pd.to_datetime(deposits.block_timestamp)
  deposits['month'] = deposits.block_timestamp.dt.strftime('%Y-%m')
  deposits = deposits.drop('block_timestamp', axis = 1)
  deposits = deposits.groupby(['month', 'stake_pool_name']).sum().reset_index()

  actions = df["action"].isin(['withdraw', 'withdraw_stake', 'withdraw_dao', 'withdraw_dao_stake', 'claim'])
  cols = ['stake_pool_name', 'tx_id', 'block_timestamp',
          'succeeded', 'action', 'amount']
  withdrawals = df.loc[actions, cols]

  withdrawals['amount'] = withdrawals['amount'].astype('float')/10**9
  withdrawals = withdrawals[(withdrawals.succeeded == True)]
  withdrawals['block_timestamp'] = pd.to_datetime(withdrawals.block_timestamp)
  withdrawals['month'] = withdrawals.block_timestamp.dt.strftime('%Y-%m')
  withdrawals = withdrawals.drop('block_timestamp', axis = 1)
  withdrawals = withdrawals.groupby(['month', 'stake_pool_name']).sum().reset_index()

  net = deposits.merge(withdrawals, how='outer', left_on = ['month', 'stake_pool_name'], right_on = ['month', 'stake_pool_name'])
  net = net.fillna(0)
  net = net.rename(columns={'amount_x': 'deposit', 'amount_y': 'withdraw'})

  net['net_deposit'] = net['deposit'] - net['withdraw']

  #net['cumulative_net_deposit'] = net['deposit'].cumsum()
  net['cumulative_net_deposit'] = net.groupby(['stake_pool_name'])['net_deposit'].apply(lambda x: x.cumsum())
  net.sort_values(by = 'month', ascending = True)


def dropdown(df): # Dropdown
  df.stake_pool_name = df.stake_pool_name.str.title()
  option = st.selectbox(
      'Select Staking Pool',
      df['stake_pool_name'].unique())
  # st.write('You selected:', option)
  return option

def dd_overview(df): # Dropdown
  option = st.selectbox(
      'Choose a Metric',
      ['SOL Staked'])
  # st.write('You selected:', option)
  return option

def dropdown2(df): # Dropdown
  df.stake_pool_name = df.stake_pool_name.str.title()
  option = st.selectbox(
      'Select Date Range',
      [7,15,30,60,90,180])
  # st.write('You selected:', option)
  return option

def c_net_deposit(df):
  actions = df["action"].isin(['deposit', 'deposit_stake', 'deposit_dao', 'deposit_dao_stake'])
  cols = ['stake_pool_name', 'tx_id', 'block_timestamp',
          'succeeded', 'action', 'amount']
  deposits = df.loc[actions, cols]

  deposits['amount'] = deposits['amount'].astype('float')/10**9
  deposits = deposits[(deposits.succeeded == True)]

  deposits['block_timestamp'] = pd.to_datetime(deposits.block_timestamp)
  deposits['month'] = deposits.block_timestamp.dt.strftime('%Y-%m')
  deposits = deposits.drop('block_timestamp', axis = 1)
  deposits = deposits.groupby(['month', 'stake_pool_name']).sum().reset_index()

  actions = df["action"].isin(['withdraw', 'withdraw_stake', 'withdraw_dao', 'withdraw_dao_stake', 'claim'])
  cols = ['stake_pool_name', 'tx_id', 'block_timestamp',
          'succeeded', 'action', 'amount']
  withdrawals = df.loc[actions, cols]

  withdrawals['amount'] = withdrawals['amount'].astype('float')/10**9
  withdrawals = withdrawals[(withdrawals.succeeded == True)]
  withdrawals['block_timestamp'] = pd.to_datetime(withdrawals.block_timestamp)
  withdrawals['month'] = withdrawals.block_timestamp.dt.strftime('%Y-%m')
  withdrawals = withdrawals.drop('block_timestamp', axis = 1)
  withdrawals = withdrawals.groupby(['month', 'stake_pool_name']).sum().reset_index()

  net = deposits.merge(withdrawals, how='outer', left_on = ['month', 'stake_pool_name'], right_on = ['month', 'stake_pool_name'])
  net = net.fillna(0)
  net = net.rename(columns={'amount_x': 'deposit', 'amount_y': 'withdraw'})

  net['net_deposit'] = net['deposit'] - net['withdraw']

  #net['cumulative_net_deposit'] = net['deposit'].cumsum()
  net['cumulative_net_deposit'] = net.groupby(['stake_pool_name'])['net_deposit'].apply(lambda x: x.cumsum())
  net.sort_values(by = 'month', ascending = True)
  fig = px.bar(net, x='month', y='net_deposit', color = 'stake_pool_name', title = 'Net SOL Deposit')

  st.plotly_chart(fig, use_container_width=True)

def c_net_deposit2(df):
  actions = df["action"].isin(['deposit', 'deposit_stake', 'deposit_dao', 'deposit_dao_stake'])
  cols = ['stake_pool_name', 'tx_id', 'block_timestamp',
          'succeeded', 'action', 'amount']
  deposits = df.loc[actions, cols]

  deposits['amount'] = deposits['amount'].astype('float')/10**9
  deposits = deposits[(deposits.succeeded == True)]

  deposits['block_timestamp'] = pd.to_datetime(deposits.block_timestamp)
  deposits['month'] = deposits.block_timestamp.dt.strftime('%Y-%m')
  deposits = deposits.drop('block_timestamp', axis = 1)
  deposits = deposits.groupby(['month', 'stake_pool_name']).sum().reset_index()

  actions = df["action"].isin(['withdraw', 'withdraw_stake', 'withdraw_dao', 'withdraw_dao_stake', 'claim'])
  cols = ['stake_pool_name', 'tx_id', 'block_timestamp',
          'succeeded', 'action', 'amount']
  withdrawals = df.loc[actions, cols]

  withdrawals['amount'] = withdrawals['amount'].astype('float')/10**9
  withdrawals = withdrawals[(withdrawals.succeeded == True)]
  withdrawals['block_timestamp'] = pd.to_datetime(withdrawals.block_timestamp)
  withdrawals['month'] = withdrawals.block_timestamp.dt.strftime('%Y-%m')
  withdrawals = withdrawals.drop('block_timestamp', axis = 1)
  withdrawals = withdrawals.groupby(['month', 'stake_pool_name']).sum().reset_index()

  net = deposits.merge(withdrawals, how='outer', left_on = ['month', 'stake_pool_name'], right_on = ['month', 'stake_pool_name'])
  net = net.fillna(0)
  net = net.rename(columns={'amount_x': 'deposit', 'amount_y': 'withdraw'})

  net['net_deposit'] = net['deposit'] - net['withdraw']

  #net['cumulative_net_deposit'] = net['deposit'].cumsum()
  net['cumulative_net_deposit'] = net.groupby(['stake_pool_name'])['net_deposit'].apply(lambda x: x.cumsum())
  net.sort_values(by = 'month', ascending = True)
  # fig = px.bar(net, x='month', y='net_deposit', color = 'stake_pool_name', title = 'net_deposit')
  fig2 = px.bar(net, x='month', y='cumulative_net_deposit', color = 'stake_pool_name', title = 'Net SOL Deposit - Cumulative')

  # st.plotly_chart(fig, use_container_width=True)
  st.plotly_chart(fig2, use_container_width=True)

def update_button_callback():
  duration = 0
  # st.write('Latest data is', max(df.block_timestamp)) 
  latest = datetime.strptime(max(df.block_timestamp)[:-4], '%Y-%m-%d %H:%M:%S')
  duration = np.round((datetime.now() - latest).total_seconds() /3600, 2)
  st.write('Last Update was', duration, 'hours ago')

  if duration > st.secrets['update_delay']:
    st.write('Latest data was more than', st.secrets['update_delay'], 'hours ago!')
    st.text('Fetching new data. This may take a while ...')
    update_data()
    st.text('All Data is up to date!')
  else: # data was recently updated
    st.text('Data is up to date! There is no need to fetch.')


#LOAD CSVs
df = load_data()

#DEPLOY WIDGETS
overview, comparison, user_analysis, about = st.tabs(["Overview", 'Comparison', 'User Analysis', "About"])

with overview:
  with st.container():
    st.header('Overview')
    dd_overview(df) 

  col1, col2 = st.columns(2)

  col3, col4 = st.columns([1,2])

  col5, col6, col7 = st.columns([1,1,2])



  with st.container():
    with col1:      
      c_net_deposit(df)

    with col2:      
      c_net_deposit2(df)


  
  


with about:
  st.write("### Dashboard by ")
  st.write('[h4wk](https://twitter.com/h4wk10)')
  st.write('[banbannard](https://twitter.com/banbannard)')
  st.write('[Raine](https://twitter.com/0xSinten)')
  st.write('Data from [Flipside Crypto](https://flipsidecrypto.xyz/)')
  st.button('Update Data' , on_click = update_button_callback, help="Check for the latest database update in the last 24 hours") 






