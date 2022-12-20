
import time
from datetime import datetime
import pyarrow
import pandas as pd
import schedule
from pydal import DAL

db = DAL('mssql3://auditoria:I2iOYd40Le134WQq1YqM@35.247.206.86/db_sinclog_hmlog')


def consultaMovimentacao():
    print("Inicio Consulta Movimentacao")

    now = datetime.now()

    nomearquivomov = f"Movimentação-{now.day}{now.month}{now.year}{now.hour}{now.minute}"

    f_ocorrencia = open("movi.txt", "r")

    query = f_ocorrencia.read()

    retorno = db.executesql(
        query, as_dict=True
    )

    df = pd.DataFrame(retorno)
    df.to_parquet(fr'''C:\GFLMOVI\{nomearquivomov}.parquet''', engine='pyarrow')
    print(f'Fim {nomearquivomov}')


def consultaRelv2():
    print("Inicio Consulta rel geral v2")

    now = datetime.now()

    nomearquivorel = f"Relgeral-{now.day}{now.month}{now.year}{now.hour}{now.minute}"

    f_ocorrencia = open("Rel2.txt", "r")

    query = f_ocorrencia.read()

    retorno = db.executesql(
        query, as_dict=True
    )

    df = pd.DataFrame(retorno)
    df.to_parquet(fr'''C:\GFLRELV2\{nomearquivorel}.parquet''', engine='pyarrow')
    print(f'Fim {nomearquivorel}')


# rotinas rel geral v2
schedule.every().day.at("03:30").do(consultaRelv2)
schedule.every().day.at("06:30").do(consultaRelv2)
schedule.every().day.at("09:30").do(consultaRelv2)
schedule.every().day.at("12:30").do(consultaRelv2)
schedule.every().day.at("15:30").do(consultaRelv2)
schedule.every().day.at("18:30").do(consultaRelv2)
schedule.every().day.at("21:30").do(consultaRelv2)
schedule.every().day.at("22:45").do(consultaRelv2)

# rotinas movimentação
schedule.every().day.at("03:00").do(consultaMovimentacao)
schedule.every().day.at("06:00").do(consultaMovimentacao)
schedule.every().day.at("09:00").do(consultaMovimentacao)
schedule.every().day.at("12:00").do(consultaMovimentacao)
schedule.every().day.at("15:00").do(consultaMovimentacao)
schedule.every().day.at("18:00").do(consultaMovimentacao)
schedule.every().day.at("21:00").do(consultaMovimentacao)
schedule.every().day.at("23:30").do(consultaMovimentacao)

while True:
    schedule.run_pending()
    time.sleep(1)