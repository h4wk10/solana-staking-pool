import streamlit as st
import pandas as pd
import numpy as np
import plotly.figure_factory as ff
import plotly.express as px
import plotly.graph_objects as go
from shroomdk import ShroomDK
from datetime import datetime
from plotly.subplots import make_subplots

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

    # GET NET DEPOSIT
    actions = df["action"].isin(['deposit', 'deposit_stake', 'deposit_dao', 'deposit_dao_stake'])
    cols = ['stake_pool_name', 'tx_id', 'block_timestamp',
            'succeeded', 'action', 'amount', 'address']
    deposits = df.loc[actions, cols]

    deposits['amount'] = deposits['amount'].astype('float')/10**9
    deposits = deposits[(deposits.succeeded == True)]

    deposits['block_timestamp'] = pd.to_datetime(deposits.block_timestamp)
    deposits['month'] = deposits.block_timestamp.dt.strftime('%Y-%m')
    deposits = deposits.drop('block_timestamp', axis = 1)
    deposits = deposits.groupby(['month', 'stake_pool_name']).sum().reset_index()

    actions = df["action"].isin(['withdraw', 'withdraw_stake', 'withdraw_dao', 'withdraw_dao_stake', 'claim'])
    cols = ['stake_pool_name', 'tx_id', 'block_timestamp',
            'succeeded', 'action', 'amount', 'address']
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
    net['stake_pool_name'] = net['stake_pool_name'].str.capitalize()
    net['cumulative_net_deposit'] = net.groupby(['stake_pool_name'])['net_deposit'].apply(lambda x: x.cumsum())
    net.sort_values(by = 'month', ascending = True)
    return df, net

def dd_stake_pool_name(df): # Dropdown
  df.stake_pool_name = df.stake_pool_name.str.title()
  option = st.selectbox(
      'Select Staking Pool',
      df['stake_pool_name'].unique())
  # st.write('You selected:', option)
  return option

def dd_overview(df): # Dropdown
  option = st.selectbox(
      'Choose a Metric',
      ['SOL Staked', 'Stake Transaction'])
  # st.write('You selected:', option)
  return option

def dd_date_range(df): # Dropdown
  df.stake_pool_name = df.stake_pool_name.str.title()
  option = st.selectbox(
      'Select Date Range',
      [7,15,30,60,90,180])
  # st.write('You selected:', option)
  return option

def c_net_deposit(net):
  fig = px.bar(net, x='month', y='net_deposit', color = 'stake_pool_name', title = 'Net SOL Deposit', color_discrete_sequence=px.colors.qualitative.Prism)
  fig.update_xaxes(showgrid=False)
  fig.update_yaxes(showgrid=False)
  st.plotly_chart(fig, use_container_width=True)

def c_net_deposit2(net):
  fig2 = px.bar(net, x='month', y='cumulative_net_deposit', color = 'stake_pool_name', title = 'Net SOL Deposit - Cumulative', color_discrete_sequence=px.colors.qualitative.Prism)
  fig2.update_xaxes(showgrid=False)
  fig2.update_yaxes(showgrid=False)
  st.plotly_chart(fig2, use_container_width=True)

def i_total_staked(net, df):
  total_staked = sum(net['net_deposit'])

  total_staker = df['address'].nunique()

  fig = go.Figure()

  fig3 = go.Indicator(
      mode = 'number',
      gauge = {'shape': "bullet"},
      # delta = {'reference': total_staked*0.95},
      value = total_staked,
      domain = {'row': 0, 'column': 0},
      title = {'text': "SOL Staked"})

  fig4 = go.Indicator(
      mode = 'number',
      gauge = {'shape': "bullet"},
      value = total_staker,
      domain = {'row': 1, 'column': 0},
      title = {'text': "Unique Stakers"})

  actions = df["action"].isin(['deposit', 'deposit_stake', 'deposit_dao', 'deposit_dao_stake'])
  cols = ['address', 'tx_id', 'block_timestamp',
          'succeeded', 'action', 'amount']
  deposits = df.loc[actions, cols]
  deposits['amount'] = deposits['amount'].astype('float')/10**9
  deposits = deposits[(deposits.succeeded == True)]
  deposits['block_timestamp'] = pd.to_datetime(deposits.block_timestamp)
  deposits['month'] = deposits.block_timestamp.dt.strftime('%Y-%m')
  deposits = deposits.drop('block_timestamp', axis = 1)
  deposits = deposits.groupby(['month']).nunique().reset_index()
  deposits['stake_transactions'] = deposits['tx_id']
  actions = df["action"].isin(['withdraw', 'withdraw_stake', 'withdraw_dao', 'withdraw_dao_stake', 'claim'])
  cols = ['address', 'tx_id', 'block_timestamp',
          'succeeded', 'action', 'amount']
  withdrawals = df.loc[actions, cols]
  withdrawals['amount'] = withdrawals['amount'].astype('float')/10**9
  withdrawals = withdrawals[(withdrawals.succeeded == True)]
  withdrawals['block_timestamp'] = pd.to_datetime(withdrawals.block_timestamp)
  withdrawals['month'] = withdrawals.block_timestamp.dt.strftime('%Y-%m')
  withdrawals = withdrawals.drop('block_timestamp', axis = 1)
  withdrawals = withdrawals.groupby(['month']).nunique().reset_index()
  withdrawals['unstake_transactions'] = withdrawals['tx_id']
  stake_tx = sum(deposits['stake_transactions'])

  fig5 = go.Indicator(
      mode = 'number',
      gauge = {'shape': "bullet"},
      #delta = {'reference': 0},
      value = stake_tx,
    # color='#1f77b4',
      domain = {'row': 2, 'column': 0},
      title = {'text': "SOL Stake Transactions"})

  unstake_tx = sum(withdrawals['unstake_transactions'])

  fig6 = go.Indicator(
      mode = 'number',
      gauge = {'shape': "bullet"},
      #delta = {'reference': 0},
      value = unstake_tx,
    # color='#1f77b4',
      domain = {'row': 3, 'column': 0},
      title = {'text': "SOL Unstake Transactions"})

  # fig3.show()
  fig.add_trace(fig3)
  fig.add_trace(fig4)
  fig.add_trace(fig5)
  fig.add_trace(fig6)

  fig.update_layout(
    grid = {'rows': 4, 'columns': 1, 'pattern': "independent"}, height=1000)
      
  st.plotly_chart(fig, use_container_width=True)

def c_market_share2(net):
  net2 = net.groupby(by = 'month').sum()

  monthly_net = net.merge(net2, how='outer', left_on = ['month'], right_on = ['month'])
  monthly_net['market_share'] =  monthly_net['cumulative_net_deposit_x'] / monthly_net['cumulative_net_deposit_y'] * 100
  monthly_net = monthly_net[['month', 'stake_pool_name', 'market_share']]
  recent_month = max(monthly_net['month'])
  recent_month_data = monthly_net[(monthly_net.month == recent_month)]
  recent_month_data = recent_month_data[(recent_month_data.market_share == max(recent_month_data.market_share))].reset_index()
  recent_month_data = recent_month_data[['month', 'stake_pool_name', 'market_share']]
  recent_month_data['market_share'] = recent_month_data['market_share'].astype('float')
  recent_month_data['stake_pool_name'] = recent_month_data['stake_pool_name'].astype('string').str.capitalize()

  fig5 = go.Figure(go.Indicator(
      mode = 'number',
      gauge = {'shape': "bullet"},
      #delta = {'reference': 0},
      value = recent_month_data['market_share'].iloc[0],
    # color='#1f77b4',
      domain = {'x': [0, 1], 'y': [0, 1]},
      number = {"suffix": "%"},
      title = {'text': 'Top Market Share : ' + recent_month_data['stake_pool_name'].iloc[0]}))
  

  df = monthly_net[monthly_net.stake_pool_name == recent_month_data['stake_pool_name'].iloc[0]].reset_index()
  fig5.add_trace(go.Scatter(
      x = df['month'], y = df['market_share'], name=recent_month_data['stake_pool_name'].iloc[0]))
  fig5.data[1].line.color = 'purple'
  fig5.update_xaxes(title_text='Date')
  fig5.update_yaxes(title_text='Market Share in %')
  fig5.update_layout(title="Top Market Share Stake Pool (in %)")
  fig5.update_xaxes(showgrid=False)
  fig5.update_yaxes(showgrid=False)
  st.plotly_chart(fig5, use_container_width=True)

def c_market_share(net):
  net2 = net.groupby(by = 'month').sum()

  monthly_net = net.merge(net2, how='outer', left_on = ['month'], right_on = ['month'])
  monthly_net['market_share'] =  monthly_net['cumulative_net_deposit_x'] / monthly_net['cumulative_net_deposit_y'] * 100
  monthly_net = monthly_net[['month', 'stake_pool_name', 'market_share']]
  fig4 = px.area(monthly_net, x='month', y='market_share', color = 'stake_pool_name', title = 'Stake Pool Market Share by SOL Staked', color_discrete_sequence=px.colors.qualitative.Prism)
  fig4.update_xaxes(showgrid=False)
  fig4.update_yaxes(showgrid=False)
  st.plotly_chart(fig4, use_container_width=True)

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

def c_deposits_and_withdrawals_cumu(df):
  actions = df["action"].isin(['deposit', 'deposit_stake', 'deposit_dao', 'deposit_dao_stake'])
  cols = ['address', 'tx_id', 'block_timestamp',
          'succeeded', 'action', 'amount']
  deposits = df.loc[actions, cols]

  deposits['amount'] = deposits['amount'].astype('float')/10**9
  deposits = deposits[(deposits.succeeded == True)]
  deposits['block_timestamp'] = pd.to_datetime(deposits.block_timestamp)
  deposits['month'] = deposits.block_timestamp.dt.strftime('%Y-%m')
  deposits = deposits.drop('block_timestamp', axis = 1)
  deposits = deposits.groupby(['month']).nunique().reset_index()
  deposits['stake_transactions'] = deposits['tx_id']

  actions = df["action"].isin(['withdraw', 'withdraw_stake', 'withdraw_dao', 'withdraw_dao_stake', 'claim'])
  cols = ['stake_pool_name', 'tx_id', 'block_timestamp',
          'succeeded', 'action', 'amount']
  withdrawals = df.loc[actions, cols]

  withdrawals['amount'] = withdrawals['amount'].astype('float')/10**9
  withdrawals = withdrawals[(withdrawals.succeeded == True)]
  withdrawals['block_timestamp'] = pd.to_datetime(withdrawals.block_timestamp)
  withdrawals['month'] = withdrawals.block_timestamp.dt.strftime('%Y-%m')
  withdrawals = withdrawals.drop('block_timestamp', axis = 1)
  withdrawals = withdrawals.groupby(['month']).nunique().reset_index()
  withdrawals['unstake_transactions'] = withdrawals['tx_id']
  #fig = px.bar(withdrawals, x='month', y='unstake_transactions', title = 'Unstake Transactions')
  #fig.show()

  deposits['cumulative_deposit'] = deposits['stake_transactions'].cumsum()
  withdrawals['cumulative_withdrawals'] = withdrawals['unstake_transactions'].cumsum()

  fig2 = make_subplots(rows=1, cols=1)
  fig2.add_trace(go.Bar(name = "Stake Transactions", x=deposits['month'], y=deposits["cumulative_deposit"]), row=1, col=1)
  fig2.add_trace(go.Bar(name = "Unstake Transactions", x=withdrawals['month'], y=withdrawals["cumulative_withdrawals"]), row=1, col=1)
  fig2.update_layout(title = 'Stake and Unstake Transactions by Month')
  fig2.update_layout(barmode='stack')
  fig2.update_xaxes(showgrid=False)
  fig2.update_yaxes(showgrid=False)
  st.plotly_chart(fig2, use_container_width=True)

def c_deposits_and_withdrawals(df):
  actions = df["action"].isin(['deposit', 'deposit_stake', 'deposit_dao', 'deposit_dao_stake'])
  cols = ['address', 'tx_id', 'block_timestamp',
          'succeeded', 'action', 'amount']
  deposits = df.loc[actions, cols]

  deposits['amount'] = deposits['amount'].astype('float')/10**9
  deposits = deposits[(deposits.succeeded == True)]
  deposits['block_timestamp'] = pd.to_datetime(deposits.block_timestamp)
  deposits['month'] = deposits.block_timestamp.dt.strftime('%Y-%m')
  deposits = deposits.drop('block_timestamp', axis = 1)
  deposits = deposits.groupby(['month']).nunique().reset_index()
  deposits['stake_transactions'] = deposits['tx_id']

  actions = df["action"].isin(['withdraw', 'withdraw_stake', 'withdraw_dao', 'withdraw_dao_stake', 'claim'])
  cols = ['stake_pool_name', 'tx_id', 'block_timestamp',
          'succeeded', 'action', 'amount']
  withdrawals = df.loc[actions, cols]

  withdrawals['amount'] = withdrawals['amount'].astype('float')/10**9
  withdrawals = withdrawals[(withdrawals.succeeded == True)]
  withdrawals['block_timestamp'] = pd.to_datetime(withdrawals.block_timestamp)
  withdrawals['month'] = withdrawals.block_timestamp.dt.strftime('%Y-%m')
  withdrawals = withdrawals.drop('block_timestamp', axis = 1)
  withdrawals = withdrawals.groupby(['month']).nunique().reset_index()
  withdrawals['unstake_transactions'] = withdrawals['tx_id']
  #fig = px.bar(withdrawals, x='month', y='unstake_transactions', title = 'Unstake Transactions')
  #fig.show()

  fig = make_subplots(rows=1, cols=1)
  fig.add_trace(go.Bar(name = "Stake Transactions", x=deposits['month'], y=deposits["stake_transactions"]), row=1, col=1)
  fig.add_trace(go.Bar(name = "Unstake Transactions", x=withdrawals['month'], y=withdrawals["unstake_transactions"]), row=1, col=1)
  fig.update_layout(title = 'Stake and Unstake Transactions by Month')
  fig.update_layout(barmode='stack')
  fig.update_xaxes(showgrid=False)
  fig.update_yaxes(showgrid=False)
  st.plotly_chart(fig, use_container_width=True)

def c_stake_transaction_market_share(df):
  actions = df["action"].isin(['deposit', 'deposit_stake', 'deposit_dao', 'deposit_dao_stake'])
  cols = ['stake_pool_name', 'tx_id', 'block_timestamp',
          'succeeded', 'action', 'amount']
  deposits = df.loc[actions, cols]

  deposits['amount'] = deposits['amount'].astype('float')/10**9
  deposits = deposits[(deposits.succeeded == True)]

  deposits['block_timestamp'] = pd.to_datetime(deposits.block_timestamp)
  deposits['month'] = deposits.block_timestamp.dt.strftime('%Y-%m')
  deposits = deposits.drop('block_timestamp', axis = 1)
  deposits = deposits.groupby(['month', 'stake_pool_name']).nunique().reset_index()
  deposits['stake_pool_name'] = deposits['stake_pool_name'].str.capitalize()
  deposits['cumulative_deposit_tx'] = deposits.groupby(['stake_pool_name'])['tx_id'].apply(lambda x: x.cumsum())

  deposits2 = deposits.groupby(by = 'month').sum()
  monthly_deposits = deposits.merge(deposits2, how='outer', left_on = ['month'], right_on = ['month'])
  monthly_deposits['market_share'] =  monthly_deposits['cumulative_deposit_tx_x'] / monthly_deposits['cumulative_deposit_tx_y'] * 100
  monthly_deposits = monthly_deposits[['month', 'stake_pool_name', 'market_share']]
  fig2 = px.area(monthly_deposits, x='month', y='market_share', color = 'stake_pool_name', title = 'Stake Pool Market Share by Cumulative Stake Transactions')
  fig2.update_xaxes(showgrid=False)
  fig2.update_yaxes(showgrid=False)
  st.plotly_chart(fig2, use_container_width=True)

def c_top_share_stake_tx(df):
  actions = df["action"].isin(['deposit', 'deposit_stake', 'deposit_dao', 'deposit_dao_stake'])
  cols = ['stake_pool_name', 'tx_id', 'block_timestamp',
          'succeeded', 'action', 'amount']
  deposits = df.loc[actions, cols]

  deposits['amount'] = deposits['amount'].astype('float')/10**9
  deposits = deposits[(deposits.succeeded == True)]

  deposits['block_timestamp'] = pd.to_datetime(deposits.block_timestamp)
  deposits['month'] = deposits.block_timestamp.dt.strftime('%Y-%m')
  deposits = deposits.drop('block_timestamp', axis = 1)
  deposits = deposits.groupby(['month', 'stake_pool_name']).nunique().reset_index()
  deposits['stake_pool_name'] = deposits['stake_pool_name'].str.capitalize()
  deposits['cumulative_deposit_tx'] = deposits.groupby(['stake_pool_name'])['tx_id'].apply(lambda x: x.cumsum())

  deposits2 = deposits.groupby(by = 'month').sum()
  monthly_deposits = deposits.merge(deposits2, how='outer', left_on = ['month'], right_on = ['month'])
  monthly_deposits['market_share'] =  monthly_deposits['cumulative_deposit_tx_x'] / monthly_deposits['cumulative_deposit_tx_y'] * 100
  monthly_deposits = monthly_deposits[['month', 'stake_pool_name', 'market_share']]
  recent_month = max(monthly_deposits['month'])
  recent_month_data = monthly_deposits[(monthly_deposits.month == recent_month)]
  recent_month_data = recent_month_data[(recent_month_data.market_share == max(recent_month_data.market_share))].reset_index()
  recent_month_data = recent_month_data[['month', 'stake_pool_name', 'market_share']]
  recent_month_data['market_share'] = recent_month_data['market_share'].astype('float')
  recent_month_data['stake_pool_name'] = recent_month_data['stake_pool_name'].astype('string').str.capitalize()


  fig5 = go.Figure(go.Indicator(
      mode = 'number',
      gauge = {'shape': "bullet"},
      #delta = {'reference': 0},
      value = recent_month_data['market_share'].iloc[0],
    # color='#1f77b4',
      domain = {'x': [0, 1], 'y': [0, 1]},
      title = {'text': 'Top Market Share : ' + recent_month_data['stake_pool_name'].iloc[0]}))

  df = monthly_deposits[monthly_deposits.stake_pool_name == recent_month_data['stake_pool_name'].iloc[0]].reset_index()
  fig5.add_trace(go.Scatter(
      x = df['month'], y = df['market_share'], name=recent_month_data['stake_pool_name'].iloc[0]))
  fig5.update_xaxes(title_text='Date')
  fig5.update_yaxes(title_text='Market Share in %')
  fig5.update_layout(title="Top Market Share Stake Pool (in %)")
  fig5.data[1].line.color = '#656EF2'
  fig5.update_xaxes(showgrid=False)
  fig5.update_yaxes(showgrid=False)
  st.plotly_chart(fig5, use_container_width=True)

def c_net_stake_total(net):
  net2 = net.groupby(by = ['month']).sum().reset_index()
  net2['Inflow&Outflow'] = net2['net_deposit'].apply(lambda x: 'Positive Net Stake' if x > 0 else 'Negative Net Stake')


  fig = px.bar(net2, x='month', y='net_deposit', color = 'Inflow&Outflow', title = 'Net SOL Staked - Monthly', color_discrete_sequence=px.colors.qualitative.Prism)
  fig.update_xaxes(showgrid=False)
  fig.update_yaxes(showgrid=False)
  st.plotly_chart(fig, use_container_width=True)

def c_net_stake_total_cumsum(net):
  net2 = net.groupby(by = ['month']).sum().reset_index()
  net2['cumulative_net_deposit'] = net2['net_deposit'].cumsum()

  fig = px.bar(net2, x='month', y='cumulative_net_deposit', title = 'Net SOL Staked - Cumulative', color_discrete_sequence=px.colors.qualitative.Prism)
  fig.update_xaxes(showgrid=False)
  fig.update_yaxes(showgrid=False)
  st.plotly_chart(fig, use_container_width=True)

  net2['cumulative_net_deposit'] = net2['net_deposit'].cumsum()

#LOAD CSVs
df, net = load_data()

#DEPLOY WIDGETS
overview, comparison, user_analysis, about = st.tabs(["Overview", 'Comparison', 'User Analysis', "About"])



with overview:
  st.header('Stake Pool')
  option = dd_overview(df) 

  # col61, col62 = st.columns([3,2]) 
  # col21, col22 = st.columns(2)
  # col31, col32, col33 = st.columns(3)
  # col41, col42, col43, col44 = st.columns(4)
  col51, col52, col53 = st.columns([1,2,2])

  with st.container():
    with col51:    
      i_total_staked(net, df)     

    with col52:      

      if option == 'SOL Staked':
        # c_net_deposit2(net)
        # c_net_deposit(net)
        c_net_stake_total(net)
        c_net_stake_total_cumsum(net)
        

      elif option == 'Stake Transaction':
        c_deposits_and_withdrawals(df)
        c_deposits_and_withdrawals_cumu(df)

    with col53:
      if option == 'SOL Staked':
        c_market_share2(net)
        c_market_share(net)
      # c_net_deposit(net)
      elif option == 'Stake Transaction':
        c_top_share_stake_tx(df)
        c_stake_transaction_market_share(df)
        

with about:
  st.write("### Dashboard by ")
  st.write('[h4wk](https://twitter.com/h4wk10)')
  st.write('[banbannard](https://twitter.com/banbannard)')
  st.write('[Raine](https://twitter.com/0xSinten)')
  st.write('Data from [Flipside Crypto](https://flipsidecrypto.xyz/)')
  st.button('Update Data' , on_click = update_button_callback, help="Check for the latest database update in the last 24 hours") 






