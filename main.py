#!/usr/bin/python3

import psycopg2
import pandas as pd
import os

#Создание подключения
conn_bank = psycopg2.connect(database = "bank",
                        host =     "de-edu-db.chronosavant.ru",
                        user =     "bank_etl",
                        password = "********",
                        port =     "5432")

conn_edu = psycopg2.connect(database = "edu",
                        host =     "de-edu-db.chronosavant.ru",
                        user =     "de10",
                        password = "*********",
                        port =     "5432")

#Отключение автокоммита
conn_bank.autocommit = False
conn_edu.autocommit = False

#Создание курсора
cursor_bank = conn_bank.cursor()
cursor_edu = conn_edu.cursor()


#Очиста стейджинга
cursor_edu.execute("delete from de10.kvrk_stg_transactions")
cursor_edu.execute("delete from de10.kvrk_stg_terminals")
cursor_edu.execute("delete from de10.kvrk_stg_blacklist")
cursor_edu.execute("delete from de10.kvrk_stg_cards")
cursor_edu.execute("delete from de10.kvrk_stg_accounts")
cursor_edu.execute("delete from de10.kvrk_stg_clients")

#Дата отчета
for f in os.listdir():
    if f[:9] == 'terminals':
        report_dt = f[10:18]

#Запись в стейджинг
#transactions
df = pd.read_csv( '/home/de10/kvrk/project/transactions_' + report_dt + '.txt', sep=';')

cursor_edu.executemany( """INSERT INTO de10.kvrk_stg_transactions(
                                trans_id, 
                                trans_date, 
                                amt,
                                card_num, 
                                oper_type,  
                                oper_result, 
                                terminal ) 
                            VALUES( %s, %s, %s, %s, %s, %s, %s )""", df.values.tolist() );

#terminals
df = pd.read_excel( '/home/de10/kvrk/project/terminals_' + report_dt + '.xlsx', sheet_name='terminals', header=0, index_col=None )

cursor_edu.executemany( """INSERT INTO de10.kvrk_stg_terminals( 
                                terminal_id, 
                                terminal_type, 
                                terminal_city, 
                                terminal_address ) 
                            VALUES( %s, %s, %s, %s )""", df.values.tolist() );

#blacklist
df = pd.read_excel( '/home/de10/kvrk/project/passport_blacklist_' + report_dt + '.xlsx', sheet_name='blacklist', header=0, index_col=None )

cursor_edu.executemany( """INSERT INTO de10.kvrk_stg_blacklist( 
                                entry_dt,
                                passport_num ) 
                            VALUES( %s, %s )""", df.values.tolist() );

#clients
cursor_bank.execute( """SELECT
                            client_id,
                            last_name,
                            first_name,
                            patronymic,
                            date_of_birth,
                            passport_num,
                            passport_valid_to,
                            phone,
                            create_dt,
                            update_dt
                        FROM info.clients""" )
records = cursor_bank.fetchall()

names = [ x[0] for x in cursor_bank.description ]
df = pd.DataFrame( records, columns = names )

cursor_edu.executemany( """INSERT INTO de10.kvrk_stg_clients(
                                client_id,
                                last_name,
                                first_name,
                                patronymic,
                                date_of_birth,
                                passport_num,
                                passport_valid_to,
                                phone,
                                create_dt,
                                update_dt ) 
                            VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )""", df.values.tolist() )


#accounts
cursor_bank.execute( """SELECT
                            account,
                            valid_to,
                            client,
                            create_dt,
                            update_dt
                        FROM info.accounts""" )
records = cursor_bank.fetchall()

names = [ x[0] for x in cursor_bank.description ]
df = pd.DataFrame( records, columns = names )

cursor_edu.executemany( """INSERT INTO de10.kvrk_stg_accounts(
                                account_num,
                                valid_to,
                                client,
                                create_dt,
                                update_dt ) 
                            VALUES( %s, %s, %s, %s, %s )""", df.values.tolist() )


#cards
cursor_bank.execute( """SELECT
                            card_num,
                            account,
                            create_dt
                        FROM info.cards""" )
records = cursor_bank.fetchall()

names = [ x[0] for x in cursor_bank.description ]
df = pd.DataFrame( records, columns = names )

cursor_edu.executemany( """INSERT INTO de10.kvrk_stg_cards(
                                card_num,
                                account_num,
                                create_dt,
                                update_dt ) 
                            VALUES( %s, %s, %s, NOW() )""", df.values.tolist() )

#загрузка из стейджинга в хранилище
#terminals
cursor_edu.execute("""INSERT INTO de10.kvrk_dwh_dim_terminals(
                            terminal_id, 
                            terminal_type, 
                            terminal_city, 
                            terminal_address,
                            create_dt,
                            update_dt) 
                      SELECT
                            ters.terminal_id,
                            ters.terminal_type,
                            ters.terminal_city,
                            ters.terminal_address,
                            to_date(%s, 'DDMMYYYY'),
                            null
                      FROM  de10.kvrk_stg_terminals ters
                      LEFT join de10.kvrk_dwh_dim_terminals tert
                      ON   ters.terminal_id=tert.terminal_id
                      WHERE tert.terminal_id is null""", [report_dt]);


cursor_edu.execute("""UPDATE de10.kvrk_dwh_dim_terminals
                        SET terminal_type=tmp.terminal_type,
                            terminal_city=tmp.terminal_city,
                            terminal_address=tmp.terminal_address,
                            update_dt=to_date(%s, 'DDMMYYYY')
                        FROM (
                            SELECT
                                tert.terminal_id,
                                ters.terminal_type,
                                ters.terminal_city,
                                ters.terminal_address,
                                ters.update_dt,
                                null
                            FROM de10.kvrk_stg_terminals ters
                            INNER join de10.kvrk_dwh_dim_terminals tert
                            ON ters.terminal_id=tert.terminal_id
                            WHERE 0=1
                                or ters.terminal_type <> tert.terminal_type or (ters.terminal_type is null and tert.terminal_type is not null) or (ters.terminal_type is not null and tert.terminal_type is null)
                                or ters.terminal_city <> tert.terminal_city or (ters.terminal_city is null and tert.terminal_city is not null) or (ters.terminal_city is not null and tert.terminal_city is null)
                                or ters.terminal_address <> tert.terminal_address or (ters.terminal_address is null and tert.terminal_address is not null) or (ters.terminal_address is not null and tert.terminal_address is null)
                        ) tmp  
                        WHERE kvrk_dwh_dim_terminals.terminal_id=tmp.terminal_id""", [report_dt]);

#cards
cursor_edu.execute("""INSERT INTO de10.kvrk_dwh_dim_cards(
                            card_num, 
                            account_num,
                            create_dt,
                            update_dt) 
                      SELECT
                            cs.card_num,
                            cs.account_num,
                            cs.create_dt,
                            cs.update_dt
                      FROM  de10.kvrk_stg_cards cs
                      LEFT join de10.kvrk_dwh_dim_cards ct
                      ON   cs.card_num=ct.card_num
                      WHERE ct.card_num is null""");


cursor_edu.execute("""UPDATE de10.kvrk_dwh_dim_cards
                        SET account_num=tmp.account_num,
                            create_dt=tmp.create_dt,
                            update_dt=tmp.update_dt
                        FROM (
                            SELECT
                                ct.card_num,
                                cs.account_num,
                                cs.create_dt,
                                cs.update_dt                                
                            FROM de10.kvrk_stg_cards cs
                            INNER join de10.kvrk_dwh_dim_cards ct
                            ON cs.card_num=ct.card_num
                            WHERE
                                cs.account_num <> ct.account_num or (cs.account_num is null and ct.account_num is not null) or (cs.account_num is not null and ct.account_num is null)
                        ) tmp  
                        WHERE kvrk_dwh_dim_cards.card_num=tmp.card_num""");

#accounts
cursor_edu.execute("""INSERT INTO de10.kvrk_dwh_dim_accounts(
                            account_num,
                            valid_to,
                            client,
                            create_dt,
                            update_dt) 
                      SELECT
                            acs.account_num,
                            acs.valid_to,
                            acs.client,
                            acs.create_dt,
                            acs.update_dt                           
                      FROM  de10.kvrk_stg_accounts acs
                      LEFT join de10.kvrk_dwh_dim_accounts at
                      ON   acs.account_num=at.account_num
                      WHERE at.account_num is null""");


cursor_edu.execute("""UPDATE de10.kvrk_dwh_dim_accounts
                        SET valid_to=tmp.valid_to,
                            client=tmp.client,
                            update_dt=tmp.update_dt
                        FROM (
                            SELECT
                                at.account_num,
                                acs.valid_to,
                                acs.client,
                                acs.update_dt                          
                            FROM de10.kvrk_stg_accounts acs
                            INNER join de10.kvrk_dwh_dim_accounts at
                            ON acs.account_num=at.account_num
                            WHERE 0=1
                                or acs.valid_to <> at.valid_to or (acs.valid_to is null and at.valid_to is not null) or (acs.valid_to is not null and at.valid_to is null)
                                or acs.client <> at.client or (acs.client is null and at.client is not null) or (acs.client is not null and at.client is null)
                        ) tmp  
                        WHERE kvrk_dwh_dim_accounts.account_num=tmp.account_num""");

#clients
cursor_edu.execute("""INSERT INTO de10.kvrk_dwh_dim_clients(
                            client_id, 
                            last_name, 
                            first_name, 
                            patronymic,
                            date_of_birth,
                            passport_num,
                            passport_valid_to,
                            phone,
                            create_dt,
                            update_dt) 
                      SELECT
                            cls.client_id,
                            cls.last_name,
                            cls.first_name,
                            cls.patronymic,
                            cls.date_of_birth,
                            cls.passport_num,
                            cls.passport_valid_to,
                            cls.phone,
                            cls.create_dt,
                            cls.update_dt
                      FROM  de10.kvrk_stg_clients cls
                      LEFT join de10.kvrk_dwh_dim_clients clt
                      ON   cls.client_id=clt.client_id
                      WHERE clt.client_id is null""");


cursor_edu.execute("""UPDATE de10.kvrk_dwh_dim_clients
                        SET last_name=tmp.last_name,
                            first_name=tmp.first_name,
                            patronymic=tmp.patronymic,
                            date_of_birth=tmp.date_of_birth,
                            passport_num=tmp.passport_num,
                            passport_valid_to=tmp.passport_valid_to,
                            phone=tmp.phone,
                            update_dt=tmp.update_dt
                        FROM (
                            SELECT
                                clt.client_id,
                                cls.last_name,
                                cls.first_name,
                                cls.patronymic,
                                cls.date_of_birth,
                                cls.passport_num,
                                cls.passport_valid_to,
                                cls.phone,
                                cls.update_dt
                            FROM de10.kvrk_stg_clients cls
                            INNER join de10.kvrk_dwh_dim_clients clt
                            ON cls.client_id=clt.client_id
                            WHERE 0=1
                                or cls.last_name <> clt.last_name or (cls.last_name is null and clt.last_name is not null) or (cls.last_name is not null and clt.last_name is null)
                                or cls.first_name <> clt.first_name or (cls.first_name is null and clt.first_name is not null) or (cls.first_name is not null and clt.first_name is null)
                                or cls.patronymic <> clt.patronymic or (cls.patronymic is null and clt.patronymic is not null) or (cls.patronymic is not null and clt.patronymic is null)
                                or cls.date_of_birth <> clt.date_of_birth or (cls.date_of_birth is null and clt.date_of_birth is not null) or (cls.date_of_birth is not null and clt.date_of_birth is null)
                                or cls.passport_num <> clt.passport_num or (cls.passport_num is null and clt.passport_num is not null) or (cls.passport_num is not null and clt.passport_num is null)
                                or cls.passport_valid_to <> clt.passport_valid_to or (cls.passport_valid_to is null and clt.passport_valid_to is not null) or (cls.passport_valid_to is not null and clt.passport_valid_to is null)
                                or cls.phone <> clt.phone or (cls.phone is null and clt.phone is not null) or (cls.phone is not null and clt.phone is null)
                        ) tmp  
                        WHERE kvrk_dwh_dim_clients.client_id=tmp.client_id""");

#Загрузка фактов
#blacklist
cursor_edu.execute("""INSERT INTO de10.kvrk_dwh_fact_passport_blacklist(
                            passport_num, 
                            entry_dt) 
                      SELECT
                            bls.passport_num,
                            bls.entry_dt
                      FROM  de10.kvrk_stg_blacklist bls
                      WHERE entry_dt = to_date(%s, 'DDMMYYYY')""", [report_dt]);

#transactions
cursor_edu.execute("""INSERT INTO de10.kvrk_dwh_fact_transactions(
                            trans_id, 
                            trans_date,
                            amt, 
                            card_num, 
                            oper_type,
                            oper_result,
                            terminal) 
                      SELECT
                            trs.trans_id,
                            to_timestamp(trs.trans_date, 'YYYY-MM-DD HH24:MI:SS'),
                            cast(replace(trs.amt, ',', '.') as numeric(8,3)),
                            trs.card_num,
                            trs.oper_type,
                            trs.oper_result,
                            trs.terminal
                      FROM  de10.kvrk_stg_transactions trs
                      WHERE trs.trans_id NOT IN (
                                    SELECT trans_id FROM de10.kvrk_dwh_fact_transactions)""");


#Составление отчетов по операциям
#При недействующем договоре
cursor_edu.execute( """ INSERT INTO de10.kvrk_rep_fraud (
                            event_dt,
                            passport,
                            fio,
                            phone,
                            event_type,
                            report_dt)
                        SELECT
                            trt.trans_date,
                            clt.passport_num,
                            (clt.last_name || ' ' || clt.first_name || ' ' || clt.patronymic),
                            clt.phone,
                            2,
                            to_date(%s, 'DDMMYYYY')
                        FROM de10.kvrk_dwh_fact_transactions trt
                        LEFT join de10.kvrk_dwh_dim_cards ct ON ct.card_num = trt.card_num
                        LEFT join de10.kvrk_dwh_dim_accounts a ON a.account_num = ct.account_num
                        LEFT join de10.kvrk_dwh_dim_clients clt ON clt.client_id = a.client
                        WHERE trt.trans_date > a.valid_to """, [report_dt])

#При просроченном или заблокированном паспорте
cursor_edu.execute( """ insert into de10.kvrk_rep_fraud (
                            event_dt,
                            passport,
                            fio,
                            phone,
                            event_type,
                            report_dt)
                        select
                            trt.trans_date,
                            clt.passport_num,
                            (clt.last_name || ' ' || clt.first_name || ' ' || clt.patronymic),
                            clt.phone,
                            1,
                            to_date(%s, 'DDMMYYYY')
                        from de10.kvrk_dwh_fact_transactions trt
                        left join de10.kvrk_dwh_dim_cards ct on ct.card_num = trt.card_num
                        left join de10.kvrk_dwh_dim_accounts a on a.account_num = ct.account_num
                        left join de10.kvrk_dwh_dim_clients clt on clt.client_id = a.client
                        where
                            clt.passport_valid_to < to_date(%s, 'DDMMYYYY')
                            or
                            clt.passport_num in (
                                select
                                    passport_num
                                from de10.kvrk_dwh_fact_passport_blacklist
                                where entry_dt <= to_date(%s, 'DDMMYYYY') ) """, [report_dt, report_dt, report_dt])


conn_edu.commit()

#Закрытие соединения
cursor_bank.close()
cursor_edu.close()

conn_bank.close()
conn_edu.close()


os.rename('/home/de10/kvrk/project/terminals_' + report_dt + '.xlsx', '/home/de10/kvrk/project/archive/terminals_' + report_dt + 'finished' + '.xlsx.backup')
os.rename('/home/de10/kvrk/project/passport_blacklist_' + report_dt + '.xlsx', '/home/de10/kvrk/project/archive/passport_blacklist_' + report_dt + 'finished' + '.xlsx.backup')
os.rename('/home/de10/kvrk/project/transactions_' + report_dt + '.txt', '/home/de10/kvrk/project/archive/transactions_' + report_dt + 'finished' + '.txt.backup')
