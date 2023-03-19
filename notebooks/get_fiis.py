import os
import warnings
import requests
import inflection
import sqlalchemy
import sqlite3
import pandas as pd
#import numpy as np
from pathlib import Path  
from datetime import datetime
warnings.filterwarnings("ignore")

# 1.0 HEADERS

def get_fiis():

    url = 'https://www.fundsexplorer.com.br/ranking'
    headers = {    
        'User-Agent': 
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_4) AppleWebKit/537.36'
            ' (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36'    
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        df = pd.read_html(response.content, encoding='utf-8')[0]

    old_columns = ['CodigoFundo', 'Setor', 'PrecoAtual', 'LiquidezDiaria',
       'Dividendo', 'DividendYield', 'DY3MAcumulado', 'DY6MAcumulado',
       'DY12MAcumulado', 'DY3MMedia', 'DY6MMedia', 'DY12MMedia',
       'DYAno', 'VariacaoPreço', 'RentabPeriodo', 'RentabAcumulada',
       'PatrimonioLíq', 'VPA', 'P_VPA', 'DYPatrimonial',
       'VariaçãoPatrimonial', 'RentabPatrPeriodo',
       'RentabPatrAcumulada', 'VacanciaFisica', 'VacanciaFinanceira',
       'QuantidadeAtivos']
    snakecase = lambda x: inflection.underscore( x )

    new_columns = list( map( snakecase, old_columns ) )

    # rename
    df.columns = new_columns
    # copy df
    df1 = df.copy()
    # drop na
    df1 = df1.dropna( axis=0, subset=['setor']  )
    # change types
    categorical_columns = ['codigo_fundo','setor']
    df1[categorical_columns] = df1[categorical_columns].astype('category')
    # pega todas as colunas que serão convertidas para o tipo float
    #col_floats = df1.select_dtypes( exclude=['category', 'int64'] )
    col_floats = list(df1.iloc[:,2:-1].columns)

    # preenche os nans com 0
    df1[col_floats] = df1[col_floats].fillna(value=0)

    # normaliza os dados deixando apenas números
    df1[col_floats] = df1[col_floats].applymap(lambda x: str(x).replace('R$', '').replace('.0','').replace('.','').replace('%','').replace(',','.'))

    # altera o tipo para float
    df1[col_floats] = df1[col_floats].astype('float')
    # normaliza o valor do P/VPA
    df1['p_vpa'] = df1['p_vpa']/100    
    return df1

# create function oportunidades
def oportunidades(tabela_full):
    aux = tabela_full
    filtros = \
    (aux['p_vpa'] < 1.0) &\
    (aux['vacancia_fisica'] == 0) &\
    (aux['dividend_yield'] > 0)
    aux2 = aux[filtros]
    #df_oportunidades = oportunidades(aux2)
    return aux2

def write_csv(  ):
    date = datetime.now().strftime('%Y%m%d')
    tb_full = get_fiis()
    tb_full.to_csv(f'../data/tb_fiis_{date}.csv', index=False, encoding='utf-8')
    tb_oportunidade = oportunidades(tb_full)
    tb_oportunidade.to_csv(f'../data/tb_oportunidades_{date}.csv', index=False, encoding='utf-8')
    print("Arquivos Exportados com sucesso!!")

write_csv() 

def load():
    # Transformar arquivos csv em tabelas de banco de dados
    DATABASE_LOCATION = "sqlite:///investimentos.sqlite"

    # Load
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('investimentos.sqlite')
    cursor = conn.cursor()

    create_table = """
    CREATE TABLE IF NOT EXISTS tb_fundos_imobiliarios(
        codigo_fundo VARCHAR(200),
        setor VARCHAR(200),
        preco_atual FLOAT(64),
        liquidez_diaria FLOAT(64),
        dividendo FLOAT(64),
        dividend_yield FLOAT(64),
        dy3_m_acumulado FLOAT(64), 
        dy6_m_acumulado FLOAT(64),
        dy12_m_acumulado FLOAT(64),
        dy3_m_media FLOAT(64),
        dy6_m_media FLOAT(64),
        dy12_m_media FLOAT(64),
        dy_ano FLOAT(64),
        variacao_preço FLOAT(64),
        rentab_periodo FLOAT(64),
        rentab_acumulada FLOAT(64),
        patrimonio_líq FLOAT(64),
        vpa FLOAT(64),
        p_vpa FLOAT(64),
        dy_patrimonial FLOAT(64),
        variação_patrimonial FLOAT(64),
        rentab_patr_periodo FLOAT(64),
        rentab_patr_acumulada FLOAT(64),
        vacancia_fisica FLOAT(64), 
        vacancia_financeira FLOAT(64),
        quantidade_ativos FLOAT(64)
    )
    """
    create_table = """
    CREATE TABLE IF NOT EXISTS tb_oportunidades(
        codigo_fundo VARCHAR(200),
        setor VARCHAR(200),
        preco_atual FLOAT(64),
        liquidez_diaria FLOAT(64),
        dividendo FLOAT(64),
        dividend_yield FLOAT(64),
        dy3_m_acumulado FLOAT(64), 
        dy6_m_acumulado FLOAT(64),
        dy12_m_acumulado FLOAT(64),
        dy3_m_media FLOAT(64),
        dy6_m_media FLOAT(64),
        dy12_m_media FLOAT(64),
        dy_ano FLOAT(64),
        variacao_preço FLOAT(64),
        rentab_periodo FLOAT(64),
        rentab_acumulada FLOAT(64),
        patrimonio_líq FLOAT(64),
        vpa FLOAT(64),
        p_vpa FLOAT(64),
        dy_patrimonial FLOAT(64),
        variação_patrimonial FLOAT(64),
        rentab_patr_periodo FLOAT(64),
        rentab_patr_acumulada FLOAT(64),
        vacancia_fisica FLOAT(64), 
        vacancia_financeira FLOAT(64),
        quantidade_ativos FLOAT(64)
    )
    """


    tb_fundos_imobiliarios = get_fiis()
    tb_fundos_imobiliarios.to_sql('fundos_imobiliarios', conn, if_exists='replace', index = False)
    tb_oportunidade = oportunidades(tb_fundos_imobiliarios)
    tb_oportunidade.to_sql('oportunidades', conn, if_exists='replace', index = False)
    print("Tabelas populadas com sucesso!!")

load()