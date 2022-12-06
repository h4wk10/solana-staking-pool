import streamlit as st
import pandas as pd
import numpy as np
import plotly.figure_factory as ff
import plotly.express as px
import plotly.graph_objects as go
from shroomdk import ShroomDK
from datetime import datetime
from plotly.subplots import make_subplots
from dateutil.relativedelta import relativedelta

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

  # UPDATE STAKER COUNT
  st.text('Updating Stakers data ...')

  sc = load_staker_count(fact_stake_pool_actions)
  scp = load_staker_count_pool(fact_stake_pool_actions)  
  
  sc = sc.reset_index(drop = True)
  sc.to_csv('data/sc.csv', index = False)

  scp = scp.reset_index(drop = True)
  scp.to_csv('data/scp.csv', index = False)

  st.text('Stakers data is up to date!')


def load_data():
    df = pd.read_csv(
        "data/fact_stake_pool_actions.csv"
    )

    #STAKER COUNT
    sc = pd.read_csv(
        "data/sc.csv"
    )

    #STAKER COUNT POOL
    scp = pd.read_csv(
        "data/scp.csv"
    )

    sol_holdings_df = pd.read_csv(
      'data/sol_holdings_df.csv'
      )

    funds_df = pd.read_csv(
      'data/df_stake_pools_all_sources.csv'
    )

    protocol_df = pd.read_csv(
      'data/protocol_interactions_df.csv'
    )

    
    return df, sc, scp, sol_holdings_df, funds_df, protocol_df

def load_net(df):
  # GET NET DEPOSIT
  actions = df["action"].isin(['deposit', 'deposit_stake', 'deposit_dao', 'deposit_dao_stake', 'deposit_dao_with_referrer'])
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
  return net

def load_staker_count(df):
  actions = df["action"].isin(['deposit', 'deposit_stake', 'deposit_dao', 'deposit_dao_stake', 'deposit_dao_with_referrer'])
  cols = ['stake_pool_name', 'address', 'tx_id', 'block_timestamp',
          'succeeded', 'action', 'amount']
  deposits = df.loc[actions, cols]

  deposits['amount'] = deposits['amount'].astype('float')/10**9
  deposits = deposits[(deposits.succeeded == True)]

  deposits['block_timestamp'] = pd.to_datetime(deposits.block_timestamp)
  deposits['month'] = deposits.block_timestamp.dt.strftime('%Y-%m')
  deposits = deposits.drop('block_timestamp', axis = 1)
  date = list(deposits['month'].unique())

  staker_count_list = []
  for i in date:
    actions = df["action"].isin(['deposit', 'deposit_stake', 'deposit_dao', 'deposit_dao_stake', 'deposit_dao_with_referrer'])
    cols = ['stake_pool_name', 'address', 'tx_id', 'block_timestamp',
            'succeeded', 'action', 'amount']
    deposits = df.loc[actions, cols]

    deposits['amount'] = deposits['amount'].astype('float')/10**9
    deposits = deposits[(deposits.succeeded == True)]

    deposits['block_timestamp'] = pd.to_datetime(deposits.block_timestamp)
    deposits['month'] = deposits.block_timestamp.dt.strftime('%Y-%m')
    deposits = deposits.drop('block_timestamp', axis = 1)
    filtered_deposit = deposits.loc[deposits['month'] <= i]

    deposits = filtered_deposit.groupby(['address', 'stake_pool_name']).sum().reset_index()

    actions = df["action"].isin(['withdraw', 'withdraw_stake', 'withdraw_dao', 'withdraw_dao_stake', 'claim'])
    cols = ['stake_pool_name', 'address', 'tx_id', 'block_timestamp',
            'succeeded', 'action', 'amount']
    withdrawals = df.loc[actions, cols]

    withdrawals['amount'] = withdrawals['amount'].astype('float')/10**9
    withdrawals = withdrawals[(withdrawals.succeeded == True)]
    withdrawals['block_timestamp'] = pd.to_datetime(withdrawals.block_timestamp)
    withdrawals['month'] = withdrawals.block_timestamp.dt.strftime('%Y-%m')

    withdrawals = withdrawals.drop('block_timestamp', axis = 1)

    filtered_withdrawals = withdrawals.loc[withdrawals['month'] <= i]
    withdrawals = filtered_withdrawals.groupby(['address', 'stake_pool_name']).sum().reset_index()


  # Net Deposits and Withdrawals
    net = deposits.merge(withdrawals, how='left', left_on = ['address', 'stake_pool_name'], right_on = ['address', 'stake_pool_name']).drop(['succeeded_x', 'succeeded_y', 'stake_pool_name'], axis = 1)
    net = net.fillna(0)
    net = net.rename(columns={'amount_x': 'deposit', 'amount_y': 'withdraw'})
    net['net_deposit'] = net['deposit'] - net['withdraw']
    net = net.groupby(by = ['address']).sum().reset_index()
    net['staker_status'] = net['net_deposit'].apply(lambda x: 1 if x > 0 else 0)
    staker_count_list.append(net['staker_status'].sum())

  # Staker Count
  staker_count = {
      'month' : date,
      'staker_count' : staker_count_list
  }

  staker_count_df = pd.DataFrame(staker_count)
  return staker_count_df

def load_staker_count_pool(df):
  actions = df["action"].isin(['deposit', 'deposit_stake', 'deposit_dao', 'deposit_dao_stake', 'deposit_dao_with_referrer'])
  cols = ['stake_pool_name', 'address', 'tx_id', 'block_timestamp',
          'succeeded', 'action', 'amount']
  deposits = df.loc[actions, cols]

  deposits['amount'] = deposits['amount'].astype('float')/10**9
  deposits = deposits[(deposits.succeeded == True)]
  deposits['block_timestamp'] = pd.to_datetime(deposits.block_timestamp)
  deposits['month'] = deposits.block_timestamp.dt.strftime('%Y-%m')
  deposits = deposits.drop('block_timestamp', axis = 1)
  date = list(deposits['month'].unique())

  date_list = []
  staker_count_list = []
  stake_pool_name_list = []

  staker_count = {
      'date_stake' : date_list,
      'staker_status' : staker_count_list,
      'stake_pool_name' : stake_pool_name_list
  }
  staker_count_df = pd.DataFrame(staker_count)

  for i in date:
    actions = df["action"].isin(['deposit', 'deposit_stake', 'deposit_dao', 'deposit_dao_stake', 'deposit_dao_with_referrer'])
    cols = ['stake_pool_name', 'address', 'tx_id', 'block_timestamp',
            'succeeded', 'action', 'amount']
    deposits = df.loc[actions, cols]

    deposits['amount'] = deposits['amount'].astype('float')/10**9
    deposits = deposits[(deposits.succeeded == True)]

    deposits['block_timestamp'] = pd.to_datetime(deposits.block_timestamp)
    deposits['month'] = deposits.block_timestamp.dt.strftime('%Y-%m')
    deposits = deposits.drop('block_timestamp', axis = 1)
    filtered_deposit = deposits.loc[deposits['month'] <= i]

    deposits = filtered_deposit.groupby(['address', 'stake_pool_name']).sum().reset_index()

    actions = df["action"].isin(['withdraw', 'withdraw_stake', 'withdraw_dao', 'withdraw_dao_stake', 'claim'])
    cols = ['stake_pool_name', 'address', 'tx_id', 'block_timestamp',
            'succeeded', 'action', 'amount']
    withdrawals = df.loc[actions, cols]

    withdrawals['amount'] = withdrawals['amount'].astype('float')/10**9
    withdrawals = withdrawals[(withdrawals.succeeded == True)]
    withdrawals['block_timestamp'] = pd.to_datetime(withdrawals.block_timestamp)
    withdrawals['month'] = withdrawals.block_timestamp.dt.strftime('%Y-%m')

    withdrawals = withdrawals.drop('block_timestamp', axis = 1)

    filtered_withdrawals = withdrawals.loc[withdrawals['month'] <= i]
    withdrawals = filtered_withdrawals.groupby(['address', 'stake_pool_name']).sum().reset_index()


  # Net Deposits and Withdrawals
    net = deposits.merge(withdrawals, how='left', left_on = ['address', 'stake_pool_name'], right_on = ['address', 'stake_pool_name']).drop(['succeeded_x', 'succeeded_y'], axis = 1)
    net = net.fillna(0)
    net = net.rename(columns={'amount_x': 'deposit', 'amount_y': 'withdraw'})
    net['date_stake'] = i
    net['net_deposit'] = net['deposit'] - net['withdraw']
    net['stake_pool_name'] = net['stake_pool_name'].str.capitalize()  
    net['staker_status'] = net['net_deposit'].apply(lambda x: 1 if x > 0 else 0)
    net = net['stake_pool_name'].groupby([net.date_stake, net.address, net.deposit, net.withdraw, net.net_deposit, net.staker_status]).apply(list).reset_index()

    net['stake_pool_name'] = [','.join(map(str, l)) for l in net['stake_pool_name']]

    staker_count_df = staker_count_df.append(net)

  staker_count_df = staker_count_df.groupby([staker_count_df.date_stake, staker_count_df.stake_pool_name]).sum().reset_index()
  return staker_count_df

def dd_stake_pool_name(df): # Dropdown
  df.stake_pool_name = df.stake_pool_name.str.title()
  option = st.selectbox(
      'Select Staking Pool',
      df['stake_pool_name'].unique())
  # st.write('You selected:', option)
  return option

def dd_stake_multiselect(df):
  options = st.multiselect(
    'Select Staking Pool(s)',
      df['stake_pool_name'].astype('string').str.capitalize().unique())

  # st.write('You selected:', options)
  return options

def dd_overview(df): # Dropdown
  option = st.selectbox(
      'Choose a Metric',
      ['SOL Staked', 'Stake Transaction', 'Staker Count'])
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

def i_total_staked(net, df, sc):
  total_staked = sum(net['net_deposit'])

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
      value = sc.loc[sc['month'] == max(sc['month'])].staker_count.values[0],
      domain = {'row': 1, 'column': 0},
      title = {'text': "Unique Stakers"})

  actions = df["action"].isin(['deposit', 'deposit_stake', 'deposit_dao', 'deposit_dao_stake', 'deposit_dao_with_referrer'])
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

def i_net_month(net):
  recent_month = max(net['month'])
  recent_month_data_net_deposit = net[(net.month == recent_month)]
  recent_month_net_deposit = sum(recent_month_data_net_deposit['net_deposit'])

  fig6 = go.Figure(go.Indicator(
      mode = 'number',
      gauge = {'shape': "bullet"},
      #delta = {'reference': 0},
      value = recent_month_net_deposit,
    # color='#1f77b4',
      domain = {'x': [0, 1], 'y': [0, 1]},
      title = {'text': "Net Staked This Month"}))
  fig6.update_layout( height=280)
  st.plotly_chart(fig6, use_container_width=True)

def i_active_wallet(df):
  df['block_timestamp'] = pd.to_datetime(df.block_timestamp)
  df['month'] = df.block_timestamp.dt.strftime('%Y-%m')
  df = df.drop('block_timestamp', axis = 1)
  df = df[(df.succeeded == True)] 
  recent_month = max(df['month']) 
  recent_month_active_wallets = df[(df.month == recent_month)]
  recent_month_active_wallets_value = recent_month_active_wallets['address'].nunique()

  fig6 = go.Figure(go.Indicator(
      mode = 'number',
      gauge = {'shape': "bullet"},
      #delta = {'reference': 0},
      value = recent_month_active_wallets_value,
    # color='#1f77b4',
      domain = {'x': [0, 1], 'y': [0, 1]},
      title = {'text': "Active Wallets This Month"}))
  fig6.update_layout( height=280)
  st.plotly_chart(fig6, use_container_width=True)

def i_new_staker(df):
  #transforms on the df
  df['amount'] = df['amount'].astype('float')/10**9
  df = df[(df.succeeded == True)]
  df['block_timestamp'] = pd.to_datetime(df.block_timestamp)
  df['month'] = df.block_timestamp.dt.strftime('%Y-%m')
  recent_month = max(df['month'])

  deposit_actions = ['deposit_stake', 'deposit', 'deposit_dao', 'deposit_dao_stake', 'deposit_dao_with_referrer']
  df.loc[df.action.isin(deposit_actions), 'action'] = 'deposit'

  withdraw_actions = ['order_unstake', 'claim', 'withdraw_stake', 'withdraw', 'withdraw_dao', 'withdraw_dao_stake']
  df.loc[df.action.isin(withdraw_actions), 'action'] = 'withdraw'

  # dt.strptime('Jun 1 2005', '%Y-%m-%d').datetime()
  month_year_filter = datetime.now().strftime('%Y-%m')
  # mask = df['block_timestamp'] <= datetime.strptime(month_year_filter, '%Y-%m-%d')
  # df_filtered = df.loc[mask]
  # df_filtered = df[(df.month == recent_month)]
  df_filtered = df[(df.month <= month_year_filter)]

  deposits_df = df_filtered[df_filtered["action"] == 'deposit']

  #group by withdraws by stake pool and address
  earliest_stake_df = deposits_df.groupby(['address', 'stake_pool_name']).agg(min_date=('month', np.min)).reset_index()

  # earliest_stake_df['min_month'] = earliest_stake_df.min_date.dt.strftime('%Y-%m')

  new_stakers = earliest_stake_df[(earliest_stake_df.min_date == month_year_filter)]

  number_stakers = new_stakers.address.nunique()
  fig3 = go.Figure(go.Indicator(
      mode = 'number',
      gauge = {'shape': "bullet"},
      #delta = {'reference': 0},
      value = number_stakers,
    # color='#1f77b4',
      domain = {'x': [0, 1], 'y': [0, 1]},
      title = {'text': "New Stakers This Month"}))
  fig3.update_layout( height=280)
  st.plotly_chart(fig3, use_container_width=True)

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
  fig5.data[1].line.color = '#5F4690'
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
  actions = df["action"].isin(['deposit', 'deposit_stake', 'deposit_dao', 'deposit_dao_stake', 'deposit_dao_with_referrer'])
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
  fig2.data[0].marker.color = '#5F4690'
  fig2.data[1].marker.color = '#1D6996'
  st.plotly_chart(fig2, use_container_width=True)

def c_deposits_and_withdrawals(df):
  actions = df["action"].isin(['deposit', 'deposit_stake', 'deposit_dao', 'deposit_dao_stake', 'deposit_dao_with_referrer'])
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
  fig.data[0].marker.color = '#5F4690'
  fig.data[1].marker.color = '#1D6996'
  st.plotly_chart(fig, use_container_width=True)

def c_stake_transaction_market_share(df):
  actions = df["action"].isin(['deposit', 'deposit_stake', 'deposit_dao', 'deposit_dao_stake', 'deposit_dao_with_referrer'])
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
  fig2 = px.area(monthly_deposits, x='month', y='market_share', color = 'stake_pool_name', title = 'Stake Pool Market Share by Cumulative Stake Transactions'
  , color_discrete_sequence=px.colors.qualitative.Prism)
  fig2.update_xaxes(showgrid=False)
  fig2.update_yaxes(showgrid=False)
  st.plotly_chart(fig2, use_container_width=True)

def c_top_share_stake_tx(df):
  actions = df["action"].isin(['deposit', 'deposit_stake', 'deposit_dao', 'deposit_dao_stake', 'deposit_dao_with_referrer'])
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
      number = {"suffix": "%"},
      domain = {'x': [0, 1], 'y': [0, 1]},
      title = {'text': 'Top Market Share : ' + recent_month_data['stake_pool_name'].iloc[0]}))

  df = monthly_deposits[monthly_deposits.stake_pool_name == recent_month_data['stake_pool_name'].iloc[0]].reset_index()
  fig5.add_trace(go.Scatter(
      x = df['month'], y = df['market_share'], name=recent_month_data['stake_pool_name'].iloc[0]))
  fig5.update_xaxes(title_text='Date')
  fig5.update_yaxes(title_text='Market Share in %')
  fig5.update_layout(title="Top Market Share Stake Pool (in %)")
  fig5.data[1].line.color = '#5F4690'
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

def c_staker_count(sc):

  fig = px.bar(sc, x='month', y='staker_count', title = 'Staker Count', color_discrete_sequence=px.colors.qualitative.Prism)
  fig.update_xaxes(showgrid=False)
  fig.update_yaxes(showgrid=False)
  st.plotly_chart(fig, use_container_width=True)

def c_staker_market_share(sc):
  net2 = sc.groupby(by = 'date_stake').sum().reset_index()

  monthly_net = sc.merge(net2, how='outer', left_on = ['date_stake'], right_on = ['date_stake'])
  monthly_net['market_share'] =  monthly_net['staker_status_x'] / monthly_net['staker_status_y'] * 100
  monthly_net = monthly_net[['date_stake', 'stake_pool_name', 'market_share']]
  fig4 = px.area(monthly_net, x='date_stake', y='market_share', color = 'stake_pool_name', title = 'Stake Pool Market Share by Staker', color_discrete_sequence=px.colors.qualitative.Prism)
  fig4.update_xaxes(showgrid=False)
  fig4.update_yaxes(showgrid=False)
  st.plotly_chart(fig4, use_container_width=True)

def c_top_staker_market_share(sc):
  net2 = sc.groupby(by = 'date_stake').sum().reset_index()
  monthly_net = sc.merge(net2, how='outer', left_on = ['date_stake'], right_on = ['date_stake'])
  monthly_net['market_share'] =  monthly_net['staker_status_x'] / monthly_net['staker_status_y'] * 100
  monthly_net = monthly_net[['date_stake', 'stake_pool_name', 'market_share']]
  recent_month = max(monthly_net['date_stake'])
  recent_month_data = monthly_net[(monthly_net.date_stake == recent_month)]
  recent_month_data = recent_month_data[(recent_month_data.market_share == max(recent_month_data.market_share))].reset_index()
  recent_month_data = recent_month_data[['date_stake', 'stake_pool_name', 'market_share']]
  recent_month_data['market_share'] = recent_month_data['market_share'].astype('float')
  recent_month_data['market_share'] = recent_month_data['market_share'].astype('float')
  recent_month_data['stake_pool_name'] = recent_month_data['stake_pool_name'].astype('string').str.capitalize()
  fig5 = go.Figure(go.Indicator(
      mode = 'number',
      gauge = {'shape': "bullet"},
      #delta = {'reference': 0},
      value = recent_month_data['market_share'].iloc[0],
    # color='#1f77b4',
      number = {"suffix": "%"},
      domain = {'x': [0, 1], 'y': [0, 1]},
      title = {'text': 'Top Market Share : ' + recent_month_data['stake_pool_name'].iloc[0]}))

  df = monthly_net[monthly_net.stake_pool_name == recent_month_data['stake_pool_name'].iloc[0]].reset_index()
  fig5.add_trace(go.Scatter(
      x = df['date_stake'], y = df['market_share']))
  fig5.update_xaxes(title_text='Date')
  fig5.update_yaxes(title_text='Market Share in %')
  fig5.update_layout(title="Top Market Share Stake Pool (in %)")
  fig5.update_xaxes(showgrid=False)
  fig5.update_yaxes(showgrid=False)
  fig5.data[1].line.color = '#5F4690'
  st.plotly_chart(fig5, use_container_width=True)

#PAGE2
def c_staker(scp, result):
  scp = scp.loc[scp['stake_pool_name'].str.contains(result, case=False)]
  fig = px.bar(scp, x='date_stake', y='staker_status', color = 'stake_pool_name', title = 'Staker Count by Stake Pool', color_discrete_sequence=px.colors.qualitative.Prism)
  fig.update_xaxes(showgrid=False)
  fig.update_yaxes(showgrid=False)
  st.plotly_chart(fig, use_container_width=True)

def c_stake_transaction(df, result):

  df = df.loc[df['stake_pool_name'].str.contains(result, case=False)]
  actions = df["action"].isin(['deposit', 'deposit_stake', 'deposit_dao', 'deposit_dao_stake', 'deposit_dao_with_referrer'])
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

  deposits['stake_transactions'] = deposits['tx_id']
  fig3 = px.bar(deposits, x='month', y='stake_transactions', color = 'stake_pool_name', title = 'Stake Transactions by Stake Pool', color_discrete_sequence=px.colors.qualitative.Prism)
  fig3.update_xaxes(showgrid=False)
  fig3.update_yaxes(showgrid=False)
  st.plotly_chart(fig3, use_container_width=True)

def c_net_stake(df, result):
  df = df.loc[df['stake_pool_name'].str.contains(result, case=False)]
  actions = df["action"].isin(['deposit', 'deposit_stake', 'deposit_dao', 'deposit_dao_stake', 'deposit_dao_with_referrer'])
  cols = ['stake_pool_name', 'tx_id', 'block_timestamp',
          'succeeded', 'action', 'amount']
  deposits = df.loc[actions, cols]

  deposits['amount'] = deposits['amount'].astype('float')/10**9
  deposits = deposits[(deposits.succeeded == True)]

  deposits['block_timestamp'] = pd.to_datetime(deposits.block_timestamp)
  deposits['month'] = deposits.block_timestamp.dt.strftime('%Y-%m')
  deposits = deposits.drop('block_timestamp', axis = 1)
  deposits = deposits.groupby(['month', 'stake_pool_name']).sum().reset_index()

  recent_month = max(deposits['month'])
  before_deposits = deposits[(deposits.month < recent_month)]

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
  net['stake_pool_name'] = net['stake_pool_name'].str.capitalize()
  #net['cumulative_net_deposit'] = net['deposit'].cumsum()
  net['cumulative_net_deposit'] = net.groupby(['stake_pool_name'])['net_deposit'].apply(lambda x: x.cumsum())
  net.sort_values(by = 'month', ascending = True)
  fig = px.bar(net, x='month', y='net_deposit', color = 'stake_pool_name', title = 'SOL Staked by Stake Pool', color_discrete_sequence=px.colors.qualitative.Prism)
  fig.update_xaxes(showgrid=False)
  fig.update_yaxes(showgrid=False)
  st.plotly_chart(fig, use_container_width=True)

def c_market_share_comparison(net, result):
  net = net.loc[net['stake_pool_name'].str.contains(result, case=False)]
  net2 = net.groupby(by = 'month').sum()

  monthly_net = net.merge(net2, how='outer', left_on = ['month'], right_on = ['month'])
  monthly_net['market_share'] =  monthly_net['cumulative_net_deposit_x'] / monthly_net['cumulative_net_deposit_y'] * 100
  monthly_net = monthly_net[['month', 'stake_pool_name', 'market_share']]
  fig4 = px.area(monthly_net, x='month', y='market_share', color = 'stake_pool_name', title = 'Stake Pool Market Share by SOL Staked', color_discrete_sequence=px.colors.qualitative.Prism)
  fig4.update_xaxes(showgrid=False)
  fig4.update_yaxes(showgrid=False)
  st.plotly_chart(fig4, use_container_width=True)

def c_stake_transaction_market_share_comparison(df, result):
  df = df.loc[df['stake_pool_name'].str.contains(result, case=False)]
  actions = df["action"].isin(['deposit', 'deposit_stake', 'deposit_dao', 'deposit_dao_stake', 'deposit_dao_with_referrer'])
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
  fig2 = px.area(monthly_deposits, x='month', y='market_share', color = 'stake_pool_name', title = 'Stake Pool Market Share by Cumulative Stake Transactions'
  , color_discrete_sequence=px.colors.qualitative.Prism)
  fig2.update_xaxes(showgrid=False)
  fig2.update_yaxes(showgrid=False)
  st.plotly_chart(fig2, use_container_width=True)

def c_staker_market_share_comparison(scp, result):
  scp = scp.loc[scp['stake_pool_name'].str.contains(result, case=False)]
  net2 = scp.groupby(by = 'date_stake').sum().reset_index()

  monthly_net = scp.merge(net2, how='outer', left_on = ['date_stake'], right_on = ['date_stake'])
  monthly_net['market_share'] =  monthly_net['staker_status_x'] / monthly_net['staker_status_y'] * 100
  monthly_net = monthly_net[['date_stake', 'stake_pool_name', 'market_share']]
  fig4 = px.area(monthly_net, x='date_stake', y='market_share', color = 'stake_pool_name', title = 'Stake Pool Market Share by Staker', color_discrete_sequence=px.colors.qualitative.Prism)
  fig4.update_xaxes(showgrid=False)
  fig4.update_yaxes(showgrid=False)
  st.plotly_chart(fig4, use_container_width=True)

#PAGE3

def dd_stake_pool(df):
  options = st.selectbox(
    'Select Staking Pool',
      df['stake_pool_name'].astype('string').str.capitalize().unique())

  # st.write('You selected:', options)
  return options

def dd_month(): # Dropdown
  months = [
    '2022-12',
    '2022-11',
    '2022-10',
    '2022-09',
    '2022-08',
    '2022-07',
    '2022-06',
    '2022-05',
    '2022-04',
    '2022-03',
    '2022-02',
    '2022-01',
    '2021-12',
    '2021-11',
    '2021-10',
    '2021-09'
]
  option = st.selectbox(
      'Select Month',
      months) 
  return option

def i_analysis_stakers(df, option_stake_pool, option_month):
  #transforms on the df
  df['amount'] = df['amount'].astype('float')/10**9
  df = df[(df.succeeded == True)]
  df['block_timestamp'] = pd.to_datetime(df.block_timestamp)
  df['month'] = df.block_timestamp.dt.strftime('%Y-%m')

  deposit_actions = ['deposit_stake', 'deposit', 'deposit_dao', 'deposit_dao_stake', 'deposit_dao_with_referrer']
  df.loc[df.action.isin(deposit_actions), 'action'] = 'deposit'

  withdraw_actions = ['order_unstake', 'claim', 'withdraw_stake', 'withdraw', 'withdraw_dao', 'withdraw_dao_stake']
  df.loc[df.action.isin(withdraw_actions), 'action'] = 'withdraw'

  # dt.strptime('Jun 1 2005', '%Y-%m-%d').datetime()
  month_year_filter = option_month #filter input
  # mask = df['block_timestamp'] <= datetime.strptime(month_year_filter, '%Y-%m-%d')
  # df_filtered = df.loc[mask]
  df_filtered = df[(df.month <= month_year_filter)]
  stake_pool = option_stake_pool.lower()
  df_filtered = df_filtered[df_filtered['stake_pool_name'] == stake_pool]


  #df of deposits and df of withdraws
  deposits_df = df_filtered[df_filtered["action"] == 'deposit']
  withdraw_df = df_filtered[df_filtered["action"] == 'withdraw']

  #group by deposits by stake pool and address
  total_deposits_wallet_df = deposits_df.groupby(['address', 'stake_pool_name', 'action'])['amount'].sum().reset_index()
  total_deposits_wallet_df.sort_values(['address'], inplace = True)

  #group by withdraws by stake pool and address
  total_withdraw_wallet_df = withdraw_df.groupby(['address', 'stake_pool_name', 'action'])['amount'].sum().reset_index()
  total_withdraw_wallet_df.sort_values(['address'], inplace = True)

  net_stake_df = total_deposits_wallet_df.merge(total_withdraw_wallet_df, on=['address', 'stake_pool_name'], how='left')

  net_stake_df = net_stake_df.rename({'amount_x': 'deposit_amount', 'amount_y': 'withdraw_amount'}, axis=1)

  net_stake_df['withdraw_amount'] = net_stake_df['withdraw_amount'].fillna(value=0)

  net_stake_df['net_stake'] = net_stake_df.apply(lambda row: row.deposit_amount - row.withdraw_amount, axis=1)

  #get whether wallet is currently staking by checking for net_stake > 0
  row_indexes=net_stake_df[net_stake_df['net_stake']>0].index
  net_stake_df.loc[row_indexes,'status']="staking"
  row_indexes=net_stake_df[net_stake_df['net_stake']<=0].index
  net_stake_df.loc[row_indexes,'status']="not staking"

  staking_df = net_stake_df[net_stake_df["status"] == 'staking']

  number_stakers = staking_df.address.nunique()

  fig3 = go.Figure(go.Indicator(
      mode = 'number',
      gauge = {'shape': "bullet"},
      #delta = {'reference': 0},
      value = number_stakers,
    # color='#1f77b4',
      domain = {'x': [0, 1], 'y': [0, 1]},
      title = {'text': "Number of Stakers"}))
  fig3.update_layout( height=280)
  st.plotly_chart(fig3, use_container_width=True)

def i_analysis_new_stakers(df, option_stake_pool, option_month):
  #transforms on the df
  df['amount'] = df['amount'].astype('float')/10**9
  df = df[(df.succeeded == True)]
  df['block_timestamp'] = pd.to_datetime(df.block_timestamp)
  df['month'] = df.block_timestamp.dt.strftime('%Y-%m')

  deposit_actions = ['deposit_stake', 'deposit', 'deposit_dao', 'deposit_dao_stake', 'deposit_dao_with_referrer']
  df.loc[df.action.isin(deposit_actions), 'action'] = 'deposit'

  withdraw_actions = ['order_unstake', 'claim', 'withdraw_stake', 'withdraw', 'withdraw_dao', 'withdraw_dao_stake']
  df.loc[df.action.isin(withdraw_actions), 'action'] = 'withdraw'

  # dt.strptime('Jun 1 2005', '%Y-%m-%d').datetime()
  month_year_filter = option_month #filter input
  # mask = df['block_timestamp'] <= datetime.strptime(month_year_filter, '%Y-%m-%d')
  # df_filtered = df.loc[mask]
  df_filtered = df[(df.month <= month_year_filter)]
  stake_pool = option_stake_pool.lower()
  df_filtered = df_filtered[df_filtered['stake_pool_name'] == stake_pool]

  deposits_df = df_filtered[df_filtered["action"] == 'deposit']

  #group by withdraws by stake pool and address
  earliest_stake_df = deposits_df.groupby(['address', 'stake_pool_name']).agg(min_date=('month', np.min)).reset_index()

  # earliest_stake_df['min_month'] = earliest_stake_df.min_date.dt.strftime('%Y-%m')

  new_stakers = earliest_stake_df[(earliest_stake_df.min_date == month_year_filter)]

  number_stakers = new_stakers.address.nunique()

  fig3 = go.Figure(go.Indicator(
      mode = 'number',
      gauge = {'shape': "bullet"},
      #delta = {'reference': 0},
      value = number_stakers,
    # color='#1f77b4',
      domain = {'x': [0, 1], 'y': [0, 1]},
      title = {'text': "Number of New Stakers"}))
  fig3.update_layout( height=280)
  st.plotly_chart(fig3, use_container_width=True)

def i_analysis_churn(df, option_stake, option_month):
  #transforms on the df
  df['amount'] = df['amount'].astype('float')/10**9
  df = df[(df.succeeded == True)]
  df['block_timestamp'] = pd.to_datetime(df.block_timestamp)
  df['month'] = df.block_timestamp.dt.strftime('%Y-%m')

  deposit_actions = ['deposit_stake', 'deposit', 'deposit_dao', 'deposit_dao_stake', 'deposit_dao_with_referrer']
  df.loc[df.action.isin(deposit_actions), 'action'] = 'deposit'

  withdraw_actions = ['order_unstake', 'claim', 'withdraw_stake', 'withdraw', 'withdraw_dao', 'withdraw_dao_stake']
  df.loc[df.action.isin(withdraw_actions), 'action'] = 'withdraw'

  # dt.strptime('Jun 1 2005', '%Y-%m-%d').datetime()
  month_year_filter = option_month #filter input
  # mask = df['block_timestamp'] <= datetime.strptime(month_year_filter, '%Y-%m-%d')
  # df_filtered = df.loc[mask]
  df_filtered = df[(df.month <= month_year_filter)]
  stake_pool = option_stake.lower()
  df_filtered = df_filtered[df_filtered['stake_pool_name'] == stake_pool]

  df_filtered['deposit_amount'] = df_filtered.apply(lambda x: x.amount if x.action == 'deposit' else -x.amount, axis=1)

  #https://stackoverflow.com/questions/25024797/max-and-min-date-in-pandas-groupby
  net_deposits_last_df = df_filtered.groupby(['stake_pool_name','address']).agg(net_deposit=('deposit_amount', np.sum), last_date=('month', np.max)).reset_index()

  zero_net_deposits_df = net_deposits_last_df[net_deposits_last_df['net_deposit'] <= 0]

  # zero_net_deposits_df['month'] = zero_net_deposits_df.last_date.dt.strftime('%Y-%m')

  zero_current_net_deposits_df = zero_net_deposits_df[(zero_net_deposits_df.last_date == month_year_filter)]

  number_churn = zero_current_net_deposits_df.address.nunique()

  fig3 = go.Figure(go.Indicator(
      mode = 'number',
      gauge = {'shape': "bullet"},
      #delta = {'reference': 0},
      value = number_churn,
    # color='#1f77b4',
      domain = {'x': [0, 1], 'y': [0, 1]},
      title = {'text': "Number of Churn"}))
  fig3.update_layout( height=280)
  st.plotly_chart(fig3, use_container_width=True)

def i_analysis_sol_holding(df, sol_holdings_df, option_stake, option_month):
  #transforms on the df
  df['amount'] = df['amount'].astype('float')/10**9
  df = df[(df.succeeded == True)]
  df['block_timestamp'] = pd.to_datetime(df.block_timestamp)
  df['month'] = df.block_timestamp.dt.strftime('%Y-%m')

  deposit_actions = ['deposit_stake', 'deposit', 'deposit_dao', 'deposit_dao_stake', 'deposit_dao_with_referrer']
  df.loc[df.action.isin(deposit_actions), 'action'] = 'deposit'

  withdraw_actions = ['order_unstake', 'claim', 'withdraw_stake', 'withdraw', 'withdraw_dao', 'withdraw_dao_stake']
  df.loc[df.action.isin(withdraw_actions), 'action'] = 'withdraw'

  # dt.strptime('Jun 1 2005', '%Y-%m-%d').datetime()
  month_year_filter = option_month #filter input
  # mask = df['block_timestamp'] <= datetime.strptime(month_year_filter, '%Y-%m-%d')
  # df_filtered = df.loc[mask]
  df_filtered = df[(df.month <= month_year_filter)]
  stake_pool = option_stake.lower()
  df_filtered = df_filtered[df_filtered['stake_pool_name'] == stake_pool]


  #df of deposits and df of withdraws
  deposits_df = df_filtered[df_filtered["action"] == 'deposit']
  withdraw_df = df_filtered[df_filtered["action"] == 'withdraw']

  #group by deposits by stake pool and address
  total_deposits_wallet_df = deposits_df.groupby(['address', 'stake_pool_name', 'action'])['amount'].sum().reset_index()
  total_deposits_wallet_df.sort_values(['address'], inplace = True)

  #group by withdraws by stake pool and address
  total_withdraw_wallet_df = withdraw_df.groupby(['address', 'stake_pool_name', 'action'])['amount'].sum().reset_index()
  total_withdraw_wallet_df.sort_values(['address'], inplace = True)

  net_stake_df = total_deposits_wallet_df.merge(total_withdraw_wallet_df, on=['address', 'stake_pool_name'], how='left')

  net_stake_df = net_stake_df.rename({'amount_x': 'deposit_amount', 'amount_y': 'withdraw_amount'}, axis=1)

  net_stake_df['withdraw_amount'] = net_stake_df['withdraw_amount'].fillna(value=0)

  net_stake_df['net_stake'] = net_stake_df.apply(lambda row: row.deposit_amount - row.withdraw_amount, axis=1)

  #get whether wallet is currently staking by checking for net_stake > 0
  row_indexes=net_stake_df[net_stake_df['net_stake']>0].index
  net_stake_df.loc[row_indexes,'status']="staking"
  row_indexes=net_stake_df[net_stake_df['net_stake']<=0].index
  net_stake_df.loc[row_indexes,'status']="not staking"

  staking_df = net_stake_df[net_stake_df["status"] == 'staking']

  current_stakers = list(set(staking_df['address'].tolist()))

  sol_holdings_df.loc[sol_holdings_df.sol_amount.isna(), 'amount_type'] = 'a. No SOL related TX for current Month (inactive)'

  sol_holdings_df['month_year'] = pd.to_datetime(sol_holdings_df.month_year).dt.strftime('%Y-%m')

  sol_holdings_df_filtered = sol_holdings_df[(sol_holdings_df.month_year == month_year_filter)]

  stakers = sol_holdings_df_filtered["wallet"].isin(current_stakers)
  sol_holdings_df_filtered = sol_holdings_df_filtered.loc[stakers]

  fig3 = go.Figure(go.Indicator(
      mode = 'number',
      gauge = {'shape': "bullet"},
      #delta = {'reference': 0},
      value = sol_holdings_df_filtered['sol_amount'].mean(),
    # color='#1f77b4',
      domain = {'x': [0, 1], 'y': [0, 1]},
      title = {'text': "Average SOL Holdings"}))
  fig3.update_layout( height=280)
  st.plotly_chart(fig3, use_container_width=True)

def c_sources_of_fund(df, funds_df, option_stake, option_month):
  #transforms on the df
  df['amount'] = df['amount'].astype('float')/10**9
  df = df[(df.succeeded == True)]
  df['block_timestamp'] = pd.to_datetime(df.block_timestamp)
  df['month'] = df.block_timestamp.dt.strftime('%Y-%m')

  deposit_actions = ['deposit_stake', 'deposit', 'deposit_dao', 'deposit_dao_stake', 'deposit_dao_with_referrer']
  df.loc[df.action.isin(deposit_actions), 'action'] = 'deposit'

  withdraw_actions = ['order_unstake', 'claim', 'withdraw_stake', 'withdraw', 'withdraw_dao', 'withdraw_dao_stake']
  df.loc[df.action.isin(withdraw_actions), 'action'] = 'withdraw'

  # dt.strptime('Jun 1 2005', '%Y-%m-%d').datetime()
  month_year_filter = option_month #filter input
  date_filter = datetime.strptime(month_year_filter, '%Y-%m')
  date_filter_last = date_filter + relativedelta(day=31)
  mask = df['block_timestamp'] <= date_filter_last
  df_filtered = df.loc[mask]
  stake_pool = option_stake.lower()
  df_filtered = df_filtered[df_filtered['stake_pool_name'] == stake_pool]


  #df of deposits and df of withdraws
  deposits_df = df_filtered[df_filtered["action"] == 'deposit']
  withdraw_df = df_filtered[df_filtered["action"] == 'withdraw']

  #group by deposits by stake pool and address
  total_deposits_wallet_df = deposits_df.groupby(['address', 'stake_pool_name', 'action'])['amount'].sum().reset_index()
  total_deposits_wallet_df.sort_values(['address'], inplace = True)

  #group by withdraws by stake pool and address
  total_withdraw_wallet_df = withdraw_df.groupby(['address', 'stake_pool_name', 'action'])['amount'].sum().reset_index()
  total_withdraw_wallet_df.sort_values(['address'], inplace = True)

  net_stake_df = total_deposits_wallet_df.merge(total_withdraw_wallet_df, on=['address', 'stake_pool_name'], how='left')

  net_stake_df = net_stake_df.rename({'amount_x': 'deposit_amount', 'amount_y': 'withdraw_amount'}, axis=1)

  net_stake_df['withdraw_amount'] = net_stake_df['withdraw_amount'].fillna(value=0)

  net_stake_df['net_stake'] = net_stake_df.apply(lambda row: row.deposit_amount - row.withdraw_amount, axis=1)

  #get whether wallet is currently staking by checking for net_stake > 0
  row_indexes=net_stake_df[net_stake_df['net_stake']>0].index
  net_stake_df.loc[row_indexes,'status']="staking"
  row_indexes=net_stake_df[net_stake_df['net_stake']<=0].index
  net_stake_df.loc[row_indexes,'status']="not staking"

  staking_df = net_stake_df[net_stake_df["status"] == 'staking']

  current_stakers = list(set(staking_df['address'].tolist()))

  stakers = funds_df["wallet"].isin(current_stakers)
  funds_df_filtered = funds_df.loc[stakers]

  funds_df_filtered['sources'] = funds_df_filtered['sources'].str.replace('Bridge', '')
  funds_df_filtered['sources'] = funds_df_filtered['sources'].str.replace('SOL Transfer', '')

  funds_count_df = funds_df_filtered.groupby(['sources']).agg(count_wallets=('wallet', 'nunique')).reset_index()

  fig2 = px.histogram(funds_count_df.sort_values(by='count_wallets', ascending = False), x='sources', y='count_wallets', color='sources',
              title='Sources of Funds', log_y= True, color_discrete_sequence=px.colors.qualitative.Prism)
  fig2.update_layout(xaxis_title='Source',
                    yaxis_title='Count of Wallet')

  fig2.update_xaxes(showgrid=False)
  fig2.update_yaxes(showgrid=False)
  st.plotly_chart(fig2, use_container_width=True)

def c_sol_holdings(df, sol_holdings_df, option_stake, option_month):
  #transforms on the df
  df['amount'] = df['amount'].astype('float')/10**9
  df = df[(df.succeeded == True)]
  df['block_timestamp'] = pd.to_datetime(df.block_timestamp)
  df['month'] = df.block_timestamp.dt.strftime('%Y-%m')

  deposit_actions = ['deposit_stake', 'deposit', 'deposit_dao', 'deposit_dao_stake', 'deposit_dao_with_referrer']
  df.loc[df.action.isin(deposit_actions), 'action'] = 'deposit'

  withdraw_actions = ['order_unstake', 'claim', 'withdraw_stake', 'withdraw', 'withdraw_dao', 'withdraw_dao_stake']
  df.loc[df.action.isin(withdraw_actions), 'action'] = 'withdraw'

  # dt.strptime('Jun 1 2005', '%Y-%m-%d').datetime()
  month_year_filter = option_month #filter input
  # mask = df['block_timestamp'] <= datetime.strptime(month_year_filter, '%Y-%m-%d')
  # df_filtered = df.loc[mask]
  df_filtered = df[(df.month <= month_year_filter)]
  stake_pool = option_stake.lower()
  df_filtered = df_filtered[df_filtered['stake_pool_name'] == stake_pool]


  #df of deposits and df of withdraws
  deposits_df = df_filtered[df_filtered["action"] == 'deposit']
  withdraw_df = df_filtered[df_filtered["action"] == 'withdraw']

  #group by deposits by stake pool and address
  total_deposits_wallet_df = deposits_df.groupby(['address', 'stake_pool_name', 'action'])['amount'].sum().reset_index()
  total_deposits_wallet_df.sort_values(['address'], inplace = True)

  #group by withdraws by stake pool and address
  total_withdraw_wallet_df = withdraw_df.groupby(['address', 'stake_pool_name', 'action'])['amount'].sum().reset_index()
  total_withdraw_wallet_df.sort_values(['address'], inplace = True)

  net_stake_df = total_deposits_wallet_df.merge(total_withdraw_wallet_df, on=['address', 'stake_pool_name'], how='left')

  net_stake_df = net_stake_df.rename({'amount_x': 'deposit_amount', 'amount_y': 'withdraw_amount'}, axis=1)

  net_stake_df['withdraw_amount'] = net_stake_df['withdraw_amount'].fillna(value=0)

  net_stake_df['net_stake'] = net_stake_df.apply(lambda row: row.deposit_amount - row.withdraw_amount, axis=1)

  #get whether wallet is currently staking by checking for net_stake > 0
  row_indexes=net_stake_df[net_stake_df['net_stake']>0].index
  net_stake_df.loc[row_indexes,'status']="staking"
  row_indexes=net_stake_df[net_stake_df['net_stake']<=0].index
  net_stake_df.loc[row_indexes,'status']="not staking"

  staking_df = net_stake_df[net_stake_df["status"] == 'staking']

  current_stakers = list(set(staking_df['address'].tolist()))

  sol_holdings_df.loc[sol_holdings_df.sol_amount.isna(), 'amount_type'] = 'a. Inactive for current Month'

  sol_holdings_df['month_year'] = pd.to_datetime(sol_holdings_df.month_year).dt.strftime('%Y-%m')

  sol_holdings_df_filtered = sol_holdings_df[(sol_holdings_df.month_year == month_year_filter)]

  stakers = sol_holdings_df_filtered["wallet"].isin(current_stakers)
  sol_holdings_df_filtered = sol_holdings_df_filtered.loc[stakers]

  sol_holdings_count_df = sol_holdings_df_filtered.groupby(['amount_type']).agg(number_interactions=('wallet', 'count')).reset_index()

  fig2 = px.histogram(sol_holdings_count_df.sort_values(by='amount_type'), x='amount_type', y='number_interactions', color='amount_type',
              title='SOL Holdings', log_y= True, color_discrete_sequence=px.colors.qualitative.Prism)
  fig2.update_layout(xaxis_title='SOL Holdings',
                    yaxis_title='Count of Wallet')
  
  fig2.update_xaxes(showgrid=False)
  fig2.update_yaxes(showgrid=False)
  st.plotly_chart(fig2, use_container_width=True)

def c_protocol_interactions(df, protocol_df, option_stake, option_month):
  #transforms on the df
  df['amount'] = df['amount'].astype('float')/10**9
  df = df[(df.succeeded == True)]
  df['block_timestamp'] = pd.to_datetime(df.block_timestamp)
  df['month'] = df.block_timestamp.dt.strftime('%Y-%m')

  deposit_actions = ['deposit_stake', 'deposit', 'deposit_dao', 'deposit_dao_stake', 'deposit_dao_with_referrer']
  df.loc[df.action.isin(deposit_actions), 'action'] = 'deposit'

  withdraw_actions = ['order_unstake', 'claim', 'withdraw_stake', 'withdraw', 'withdraw_dao', 'withdraw_dao_stake']
  df.loc[df.action.isin(withdraw_actions), 'action'] = 'withdraw'

  # dt.strptime('Jun 1 2005', '%Y-%m-%d').datetime()
  month_year_filter = option_month #filter input
  # mask = df['block_timestamp'] <= datetime.strptime(month_year_filter, '%Y-%m-%d')
  # df_filtered = df.loc[mask]
  df_filtered = df[(df.month <= month_year_filter)]
  stake_pool = option_stake.lower()
  df_filtered = df_filtered[df_filtered['stake_pool_name'] == stake_pool]


  #df of deposits and df of withdraws
  deposits_df = df_filtered[df_filtered["action"] == 'deposit']
  withdraw_df = df_filtered[df_filtered["action"] == 'withdraw']

  #group by deposits by stake pool and address
  total_deposits_wallet_df = deposits_df.groupby(['address', 'stake_pool_name', 'action'])['amount'].sum().reset_index()
  total_deposits_wallet_df.sort_values(['address'], inplace = True)

  #group by withdraws by stake pool and address
  total_withdraw_wallet_df = withdraw_df.groupby(['address', 'stake_pool_name', 'action'])['amount'].sum().reset_index()
  total_withdraw_wallet_df.sort_values(['address'], inplace = True)

  net_stake_df = total_deposits_wallet_df.merge(total_withdraw_wallet_df, on=['address', 'stake_pool_name'], how='left')

  net_stake_df = net_stake_df.rename({'amount_x': 'deposit_amount', 'amount_y': 'withdraw_amount'}, axis=1)

  net_stake_df['withdraw_amount'] = net_stake_df['withdraw_amount'].fillna(value=0)

  net_stake_df['net_stake'] = net_stake_df.apply(lambda row: row.deposit_amount - row.withdraw_amount, axis=1)

  #get whether wallet is currently staking by checking for net_stake > 0
  row_indexes=net_stake_df[net_stake_df['net_stake']>0].index
  net_stake_df.loc[row_indexes,'status']="staking"
  row_indexes=net_stake_df[net_stake_df['net_stake']<=0].index
  net_stake_df.loc[row_indexes,'status']="not staking"

  staking_df = net_stake_df[net_stake_df["status"] == 'staking']

  current_stakers = list(set(staking_df['address'].tolist()))

  protocol_interactions_df = protocol_df

  protocol_interactions_df['month_year'] = pd.to_datetime(protocol_interactions_df.month_year).dt.strftime('%Y-%m')

  protocol_interactions_df_filtered = protocol_interactions_df[(protocol_interactions_df.month_year == month_year_filter)]

  stakers = protocol_interactions_df_filtered["wallet"].isin(current_stakers)
  protocol_interactions_df_filtered = protocol_interactions_df_filtered.loc[stakers]

  protocol_interactions_count_df = protocol_interactions_df_filtered.groupby(['protocol']).agg(number_interactions=('wallet', 'count')).reset_index()

  fig2 = px.histogram(protocol_interactions_count_df.sort_values(by='number_interactions', ascending=False), x='protocol', y='number_interactions', color='protocol',
              title='Protocol Wallet Interactions', log_y= True, color_discrete_sequence=px.colors.qualitative.Prism)
  fig2.update_layout(xaxis_title='Protocol',
                    yaxis_title='Count of Wallet')
  
  fig2.update_xaxes(showgrid=False)
  fig2.update_yaxes(showgrid=False)
  st.plotly_chart(fig2, use_container_width=True)

def c_stake_pool_crossover(df, option_stake, option_month):
  #transforms on the df
  df['amount'] = df['amount'].astype('float')/10**9
  df = df[(df.succeeded == True)]
  df['block_timestamp'] = pd.to_datetime(df.block_timestamp)
  df['month'] = df.block_timestamp.dt.strftime('%Y-%m')

  deposit_actions = ['deposit_stake', 'deposit', 'deposit_dao', 'deposit_dao_stake', 'deposit_dao_with_referrer']
  df.loc[df.action.isin(deposit_actions), 'action'] = 'deposit'

  withdraw_actions = ['order_unstake', 'claim', 'withdraw_stake', 'withdraw', 'withdraw_dao', 'withdraw_dao_stake']
  df.loc[df.action.isin(withdraw_actions), 'action'] = 'withdraw'

  # dt.strptime('Jun 1 2005', '%Y-%m-%d').datetime()
  month_year_filter = option_month #filter input
  # mask = df['block_timestamp'] <= datetime.strptime(month_year_filter, '%Y-%m-%d')
  # df_filtered = df.loc[mask]
  df_filtered = df[(df.month <= month_year_filter)]
  stake_pool = option_stake.lower()
  df_filtered = df_filtered[df_filtered['stake_pool_name'] == stake_pool]


  #df of deposits and df of withdraws
  deposits_df = df_filtered[df_filtered["action"] == 'deposit']
  withdraw_df = df_filtered[df_filtered["action"] == 'withdraw']

  #group by deposits by stake pool and address
  total_deposits_wallet_df = deposits_df.groupby(['address', 'stake_pool_name', 'action'])['amount'].sum().reset_index()
  total_deposits_wallet_df.sort_values(['address'], inplace = True)

  #group by withdraws by stake pool and address
  total_withdraw_wallet_df = withdraw_df.groupby(['address', 'stake_pool_name', 'action'])['amount'].sum().reset_index()
  total_withdraw_wallet_df.sort_values(['address'], inplace = True)

  net_stake_df = total_deposits_wallet_df.merge(total_withdraw_wallet_df, on=['address', 'stake_pool_name'], how='left')

  net_stake_df = net_stake_df.rename({'amount_x': 'deposit_amount', 'amount_y': 'withdraw_amount'}, axis=1)

  net_stake_df['withdraw_amount'] = net_stake_df['withdraw_amount'].fillna(value=0)

  net_stake_df['net_stake'] = net_stake_df.apply(lambda row: row.deposit_amount - row.withdraw_amount, axis=1)

  #get whether wallet is currently staking by checking for net_stake > 0
  row_indexes=net_stake_df[net_stake_df['net_stake']>0].index
  net_stake_df.loc[row_indexes,'status']="staking"
  row_indexes=net_stake_df[net_stake_df['net_stake']<=0].index
  net_stake_df.loc[row_indexes,'status']="not staking"

  staking_df = net_stake_df[net_stake_df["status"] == 'staking']

  current_stakers = list(set(staking_df['address'].tolist()))

  df_crossover = df[(df.month <= month_year_filter)]
  # df_crossover = df[(df.month == month_year_filter)]
  df_crossover = df_crossover[df_crossover['stake_pool_name'] != stake_pool]

  stakers = df_crossover["address"].isin(current_stakers)
  df_crossover = df_crossover.loc[stakers]

  #df of deposits and df of withdraws
  deposits_crossover_df = df_crossover[df_crossover["action"] == 'deposit']
  withdraw_crossover_df = df_crossover[df_crossover["action"] == 'withdraw']

  #group by deposits by stake pool and address
  total_deposits_crossover_df = deposits_crossover_df.groupby(['address', 'stake_pool_name', 'action'])['amount'].sum().reset_index()
  total_deposits_crossover_df.sort_values(['address'], inplace = True)

  #group by withdraws by stake pool and address
  total_withdraw_crossover_df = withdraw_crossover_df.groupby(['address', 'stake_pool_name', 'action'])['amount'].sum().reset_index()
  total_withdraw_crossover_df.sort_values(['address'], inplace = True)

  net_stake_crossover_df = total_deposits_crossover_df.merge(total_withdraw_crossover_df, on=['address', 'stake_pool_name'], how='left')

  net_stake_crossover_df = net_stake_crossover_df.rename({'amount_x': 'deposit_amount', 'amount_y': 'withdraw_amount'}, axis=1)

  net_stake_crossover_df['withdraw_amount'] = net_stake_crossover_df['withdraw_amount'].fillna(value=0)

  net_stake_crossover_df['net_stake'] = net_stake_crossover_df.apply(lambda row: row.deposit_amount - row.withdraw_amount, axis=1)

  #get whether wallet is currently staking by checking for net_stake > 0
  row_indexes=net_stake_crossover_df[net_stake_crossover_df['net_stake']>0].index
  net_stake_crossover_df.loc[row_indexes,'status']="staking"
  row_indexes=net_stake_crossover_df[net_stake_crossover_df['net_stake']<=0].index
  net_stake_crossover_df.loc[row_indexes,'status']="not staking"

  staking_crossover_df = net_stake_crossover_df[net_stake_crossover_df["status"] == 'staking']

  staking_crossover_count_df = staking_crossover_df.groupby(['stake_pool_name']).agg(count_wallets=('address', 'nunique')).reset_index()
  staking_crossover_count_df['stake_pool_name'] = staking_crossover_count_df['stake_pool_name'].str.capitalize()

  fig2 = px.histogram(staking_crossover_count_df.sort_values(by='count_wallets', ascending = False), x='stake_pool_name', y='count_wallets', color='stake_pool_name',
              title='Pools Crossover User Count', log_y= True, color_discrete_sequence=px.colors.qualitative.Prism)
  fig2.update_layout(xaxis_title='Stake Pool',
                    yaxis_title='Count of Wallet')

  fig2.update_xaxes(showgrid=False)
  fig2.update_yaxes(showgrid=False)
  st.plotly_chart(fig2, use_container_width=True)

def c_stake_amount(df, option_stake, option_month):
  #transforms on the df
  df['amount'] = df['amount'].astype('float')/10**9
  df = df[(df.succeeded == True)]
  df['block_timestamp'] = pd.to_datetime(df.block_timestamp)
  df['month'] = df.block_timestamp.dt.strftime('%Y-%m')

  deposit_actions = ['deposit_stake', 'deposit', 'deposit_dao', 'deposit_dao_stake', 'deposit_dao_with_referrer']
  df.loc[df.action.isin(deposit_actions), 'action'] = 'deposit'

  withdraw_actions = ['order_unstake', 'claim', 'withdraw_stake', 'withdraw', 'withdraw_dao', 'withdraw_dao_stake']
  df.loc[df.action.isin(withdraw_actions), 'action'] = 'withdraw'

  # dt.strptime('Jun 1 2005', '%Y-%m-%d').datetime()
  month_year_filter = option_month #filter input
  # mask = df['block_timestamp'] <= datetime.strptime(month_year_filter, '%Y-%m-%d')
  # df_filtered = df.loc[mask]
  df_filtered = df[(df.month <= month_year_filter)]
  # df_filtered = df[(df.month == month_year_filter)]

  stake_pool = option_stake.lower()
  df_filtered = df_filtered[df_filtered['stake_pool_name'] == stake_pool]


  #df of deposits and df of withdraws
  deposits_df = df_filtered[df_filtered["action"] == 'deposit']
  withdraw_df = df_filtered[df_filtered["action"] == 'withdraw']

  #group by deposits by stake pool and address
  total_deposits_wallet_df = deposits_df.groupby(['address', 'stake_pool_name', 'action'])['amount'].sum().reset_index()
  total_deposits_wallet_df.sort_values(['address'], inplace = True)

  #group by withdraws by stake pool and address
  total_withdraw_wallet_df = withdraw_df.groupby(['address', 'stake_pool_name', 'action'])['amount'].sum().reset_index()
  total_withdraw_wallet_df.sort_values(['address'], inplace = True)

  net_stake_df = total_deposits_wallet_df.merge(total_withdraw_wallet_df, on=['address', 'stake_pool_name'], how='left')

  net_stake_df = net_stake_df.rename({'amount_x': 'deposit_amount', 'amount_y': 'withdraw_amount'}, axis=1)

  net_stake_df['withdraw_amount'] = net_stake_df['withdraw_amount'].fillna(value=0)

  net_stake_df['net_stake'] = net_stake_df.apply(lambda row: row.deposit_amount - row.withdraw_amount, axis=1)

  #get whether wallet is currently staking by checking for net_stake > 0
  row_indexes=net_stake_df[net_stake_df['net_stake']>0].index
  net_stake_df.loc[row_indexes,'status']="staking"
  row_indexes=net_stake_df[net_stake_df['net_stake']<=0].index
  net_stake_df.loc[row_indexes,'status']="not staking"

  staking_df = net_stake_df[net_stake_df["status"] == 'staking']

  def get_stake_amount_category(net_stake):

    if net_stake < 10:
      return "a. SOL < 10"
    elif net_stake < 100:
      return "b. 10 < SOL < 100"
    elif net_stake < 1000:
      return "c. 100 < SOL < 1,000"
    elif net_stake < 10000:
      return "d. 1,000 < SOL 10,000"
    else:
      return "e. 10,000 < SOL"
    # elif net_stake < 100000:
    #   return "e. 10,000 < SOL < 100,000"
    # elif net_stake < 1000000:
    #   return "f. 100,000 < SOL < 1,000,000"
    # else:
    #   return "g. 1M < SOL"

  staking_df['stake_amount_category'] = staking_df['net_stake'].apply(get_stake_amount_category)

  staking_cateogry_count_df = staking_df.groupby(['stake_amount_category']).agg(count_wallets=('address', 'nunique')).reset_index()

  fig2 = px.histogram(staking_cateogry_count_df, x='stake_amount_category', y='count_wallets', log_y= True,
              title='Amount of SOL Staked', color='stake_amount_category', color_discrete_sequence=px.colors.qualitative.Prism)
  fig2.update_layout(xaxis_title='Staked Amount',
                    yaxis_title='Number of Users')

  fig2.update_xaxes(showgrid=False)
  fig2.update_yaxes(showgrid=False)
  st.plotly_chart(fig2, use_container_width=True)

def c_stake_duration(df, option_stake, option_month):
  #transforms on the df
  df['amount'] = df['amount'].astype('float')/10**9
  df = df[(df.succeeded == True)]
  df['block_timestamp'] = pd.to_datetime(df.block_timestamp)
  df['month'] = df.block_timestamp.dt.strftime('%Y-%m')

  deposit_actions = ['deposit_stake', 'deposit', 'deposit_dao', 'deposit_dao_stake', 'deposit_dao_with_referrer']
  df.loc[df.action.isin(deposit_actions), 'action'] = 'deposit'

  withdraw_actions = ['order_unstake', 'claim', 'withdraw_stake', 'withdraw', 'withdraw_dao', 'withdraw_dao_stake']
  df.loc[df.action.isin(withdraw_actions), 'action'] = 'withdraw'

  # dt.strptime('Jun 1 2005', '%Y-%m-%d').datetime()
  month_year_filter = option_month #filter input
  date_filter = datetime.strptime(month_year_filter, '%Y-%m')
  date_filter_last = date_filter + relativedelta(day=31)
  mask = df['block_timestamp'] <= date_filter_last
  df_filtered = df.loc[mask]
  stake_pool = option_stake.lower()
  df_filtered = df_filtered[df_filtered['stake_pool_name'] == stake_pool]


  #df of deposits and df of withdraws
  deposits_df = df_filtered[df_filtered["action"] == 'deposit']
  withdraw_df = df_filtered[df_filtered["action"] == 'withdraw']

  #group by deposits by stake pool and address
  total_deposits_wallet_df = deposits_df.groupby(['address', 'stake_pool_name', 'action'])['amount'].sum().reset_index()
  total_deposits_wallet_df.sort_values(['address'], inplace = True)

  #group by withdraws by stake pool and address
  total_withdraw_wallet_df = withdraw_df.groupby(['address', 'stake_pool_name', 'action'])['amount'].sum().reset_index()
  total_withdraw_wallet_df.sort_values(['address'], inplace = True)

  net_stake_df = total_deposits_wallet_df.merge(total_withdraw_wallet_df, on=['address', 'stake_pool_name'], how='left')

  net_stake_df = net_stake_df.rename({'amount_x': 'deposit_amount', 'amount_y': 'withdraw_amount'}, axis=1)

  net_stake_df['withdraw_amount'] = net_stake_df['withdraw_amount'].fillna(value=0)

  net_stake_df['net_stake'] = net_stake_df.apply(lambda row: row.deposit_amount - row.withdraw_amount, axis=1)

  #get whether wallet is currently staking by checking for net_stake > 0
  row_indexes=net_stake_df[net_stake_df['net_stake']>0].index
  net_stake_df.loc[row_indexes,'status']="staking"
  row_indexes=net_stake_df[net_stake_df['net_stake']<=0].index
  net_stake_df.loc[row_indexes,'status']="not staking"

  staking_df = net_stake_df[net_stake_df["status"] == 'staking']

  current_stakers = list(set(staking_df['address'].tolist()))

  stakers = df_filtered["address"].isin(current_stakers)
  stake_duration_df = df_filtered.loc[stakers]

  stake_duration_df = stake_duration_df.groupby(['address']).agg(start_date=('block_timestamp', 'min')).reset_index()

  def get_stake_duration(start_date):

    current_date = datetime.now()

    if date_filter_last < current_date:
      return (date_filter_last - start_date).days
    else:
      return (current_date - start_date).days

  stake_duration_df['stake_duration'] = stake_duration_df['start_date'].apply(get_stake_duration)

  def get_stake_duration_category(stake_duration):

    if stake_duration < 7:
      return "a. Stake < 1 Week"
    elif stake_duration < 30:
      return "b. 1 Week < Stake < 1 Month"
    elif stake_duration < 90:
      return "c. 1 Month < Stake < 3 Months"
    elif stake_duration < 180:
      return "d. 3 Months < Stake < 6 Months"
    elif stake_duration < 360:
      return "e. 6 Months < Stake < 1 Year"
    else:
      return "f. 1 Year < Stake"

  stake_duration_df['stake_duration_category'] = stake_duration_df['stake_duration'].apply(get_stake_duration_category)

  stake_duration_cateogry_count_df = stake_duration_df.groupby(['stake_duration_category']).agg(count_wallets=('address', 'nunique')).reset_index()

  fig2 = px.histogram(stake_duration_cateogry_count_df, x='stake_duration_category', y='count_wallets',
              title='Platform Age of Staker', log_y= True, color='stake_duration_category', color_discrete_sequence=px.colors.qualitative.Prism)
  fig2.update_layout(xaxis_title='Platform Age',
                    yaxis_title='Number of Users')
  
  fig2.update_xaxes(showgrid=False)
  fig2.update_yaxes(showgrid=False)
  st.plotly_chart(fig2, use_container_width=True)

#LOAD CSVs and Create DFs
df, sc, scp, sol_holdings_df, funds_df, protocol_df = load_data()
net = load_net(df)

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
      i_total_staked(net, df, sc)     

    with col52:      

      if option == 'SOL Staked':
        # c_net_deposit2(net)
        # c_net_deposit(net)
        c_net_stake_total(net)
        c_net_stake_total_cumsum(net)
        

      elif option == 'Stake Transaction':
        c_deposits_and_withdrawals(df)
        c_deposits_and_withdrawals_cumu(df)

      elif option == 'Staker Count':
        c_staker_count(sc)


    with col53: 
      if option == 'SOL Staked':
        c_market_share2(net)
        c_market_share(net)
      # c_net_deposit(net)
      elif option == 'Stake Transaction':
        c_top_share_stake_tx(df)
        c_stake_transaction_market_share(df)

      elif option == 'Staker Count':
        c_top_staker_market_share(scp)
        c_staker_market_share(scp)
        
    col31, col32, col33 = st.columns(3)
    with col31:
      i_net_month(net)
    with col32:
      i_active_wallet(df)
    with col33:
      i_new_staker(df)

with comparison:
  st.header('Pool Comparison')
  options = dd_stake_multiselect(df)
  if not options:
    options = df['stake_pool_name'].astype('string').str.capitalize().unique()
  result = ""
  for d in options:
    result += d + '|'
  result = result[:-1]
  # st.write(result)
  p2_col21, p2_col22 = st.columns(2)
  with p2_col21:
    c_net_stake(df, result)
    c_staker(scp, result)
    c_stake_transaction(df, result)

  with p2_col22:
    c_market_share_comparison(net, result)
    c_stake_transaction_market_share_comparison(df, result)
    c_staker_market_share_comparison(scp, result)

with user_analysis:
  df, sc, scp, sol_holdings_df, funds_df, protocol_df = load_data()
  st.header('User Analysis')

  p3_col21, p3_col22 = st.columns(2)
  with p3_col21:
    option_stake_pool = dd_stake_pool(df)

  with p3_col22:
    option_month = dd_month()

  
  p3_col41, p3_col42, p3_col43, p3_col44 = st.columns(4)

  with p3_col41:
    i_analysis_stakers(df, option_stake_pool, option_month)
  
  with p3_col42:
    i_analysis_new_stakers(df, option_stake_pool, option_month)

  with p3_col43:
    i_analysis_churn(df, option_stake_pool, option_month)

  with p3_col44:
    i_analysis_sol_holding(df, sol_holdings_df, option_stake_pool, option_month)

  df, sc, scp, sol_holdings_df, funds_df, protocol_df = load_data()
  
  p3_col21, p3_col22 = st.columns(2)

  with p3_col21:
    c_stake_amount(df, option_stake_pool, option_month)
    c_sol_holdings(df, sol_holdings_df, option_stake_pool, option_month)
    c_protocol_interactions(df, protocol_df, option_stake_pool, option_month)

  with p3_col22:
    c_stake_duration(df, option_stake_pool, option_month)
    c_stake_pool_crossover(df, option_stake_pool, option_month)
    c_sources_of_fund(df, funds_df, option_stake_pool, option_month)
    
    


        

with about:
  st.write("### Dashboard by ")
  st.write('[h4wk](https://twitter.com/h4wk10)')
  st.write('[banbannard](https://twitter.com/banbannard)')
  st.write('[Raine](https://twitter.com/0xSinten)')
  st.write('Data from [Flipside Crypto](https://flipsidecrypto.xyz/)')
  st.button('Update Data' , on_click = update_button_callback, help="Check for the latest database update in the last 24 hours") 






