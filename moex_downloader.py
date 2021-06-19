import requests
import apimoex
import pandas as pd
import sys

board = 'TQBR'

df_excel = pd.read_excel(sys.argv[1])
market_type = sys.argv[2] if sys.argv[2] not is None else 'shares'
column_name = [i for i in df_excel]

df_cotir = df_excel.drop_duplicates(subset=[column_name[0], column_name[1]])
df_cotir[column_name[1]] = df_cotir[column_name[1]].map(str).map(lambda x: x[:10])
df_excel[column_name[1]] = df_excel[column_name[1]].map(str).map(lambda x: x[:10])
df_cotir[column_name[0]] = df_cotir[column_name[0]].map(str)
ticks = [line.rstrip() for line in df_cotir[column_name[0]]]
dates = [line.rstrip() for line in df_cotir[column_name[1]]]
tick_history, process = list(), 0

with requests.Session() as session:
    for i in range(len(ticks)):
        process = process + 1
        print((process / len(ticks)) * 100, '%', process)
        print(ticks[i], "=====", dates[i])

        data = apimoex.get_board_candles(session, security=ticks[i], interval=1, start=dates[i], end=dates[i], board=board, market=market_type)
        print('--')
        if data == []:
            continue
        df = pd.DataFrame(data)
        df['TICKER'] = ticks[i]
        cols = df.columns.tolist()
        cols = cols[-1:] + cols[:-1]
        df = df[cols]
        print(df.head())
        tick_history.append(df)

df_finale = pd.concat(tick_history)
df_finale['begin'] = pd.DataFrame(df_finale['begin'])
df_finale['date'] = df_finale['begin'].map(lambda x: x[:10])
df_finale['time'] = df_finale['begin'].map(lambda x: x[11:])

cols = df_finale.columns.tolist()
cols = cols[:1] + cols[-2:] + cols[1:-2]
df_finale = df_finale[cols]
df_finale.drop('begin', 1)
df_finale['date'] = df_finale['date'].map(str)
df_finale["time"] = df_finale["time"].map(str).map(lambda x: x[:5])
df_excel = df_excel.rename({column_name[2]: "time", column_name[1]: 'date', column_name[0]: 'TICKER'}, axis=1)
df_finale = pd.merge(df_finale, df_excel[['TICKER', 'date', "time"]], on=['TICKER', 'date', "time"], how='inner')
df_finale.drop_duplicates().to_csv("result.csv", index=False, sep='|')
