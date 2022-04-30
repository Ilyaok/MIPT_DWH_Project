# Получение текста запроса из sql-файла (с подстановкой имен таблиц в форматированную строку)
with open("sql_scripts/ttech.sql") as sql_file:
    query = sql_file.read()

query = query.format(
    source_table='bank.accounts')

curs.execute(query)
conn.commit()
df_test = curs.fetchall()

names = [x[0] for x in curs.description]
df_test = pd.DataFrame(result, columns=names)
print(df_test)

# Блок выгрузки таблиц из Oracle в Dataframes
curs.execute("SELECT * FROM BANK.ACCOUNTS")
conn.commit()
df_banks = curs.fetchall()

curs.execute("SELECT * FROM BANK.CARDS")
conn.commit()
df_cards = curs.fetchall()

curs.execute("SELECT * FROM BANK.CLIENTS")
conn.commit()
df_clients = curs.fetchall()

names = [x[0] for x in curs.description]
df_banks = pd.DataFrame(result, columns=names)
print(df_banks)
print(df_cards)
print(df_clients)