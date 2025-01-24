# scripts/collect_history.py
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
import concurrent.futures
import logging
from time import sleep

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stock_collection.log'),
        logging.StreamHandler()
    ]
)

# Carrega variáveis de ambiente
load_dotenv()

# Configuração do Supabase (via Postgres direto)
DB_URL = os.getenv('DATABASE_URL')

def get_last_update_date(conn, stock_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(date) FROM historical_data
            WHERE stock_id = %s
            """,
            (stock_id,)
        )
        return cur.fetchone()[0]

def insert_stock(conn, symbol, name):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO stocks (symbol, name)
            VALUES (%s, %s)
            ON CONFLICT (symbol) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            """,
            (symbol, name)
        )
        return cur.fetchone()[0]

def insert_historical_data(conn, stock_id, df):
    if df.empty:
        return 0
    
    with conn.cursor() as cur:
        data = [
            (stock_id, date.strftime('%Y-%m-%d'), row['Open'], row['High'], row['Low'], 
             row['Close'], row['Volume'], row['Low'], row['High'])
            for date, row in df.iterrows()
            if not pd.isna(row['Open']) and 
               not pd.isna(row['High']) and 
               not pd.isna(row['Low']) and 
               not pd.isna(row['Close']) and 
               not pd.isna(row['Volume']) and
               row['Volume'] > 0 and  # Garante que teve negociação
               row['Open'] > 0 and    # Garante que os preços são válidos
               row['High'] > 0 and
               row['Low'] > 0 and
               row['Close'] > 0
        ]
        
        if not data:  # Se não houver dados válidos após a filtragem
            return 0
        
        execute_values(cur,
            """
            INSERT INTO historical_data (stock_id, date, open, high, low, close, volume, min_price, max_price)
            VALUES %s
            ON CONFLICT (stock_id, date) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                min_price = EXCLUDED.min_price,
                max_price = EXCLUDED.max_price
            """,
            data,
            template="(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        return len(data)

def collect_stock_data(symbol, start_date=None):
    # Adiciona um pequeno delay para evitar sobrecarga
    sleep(0.5)
    
    # Define data mínima como 02/01/2023 se não houver data inicial
    if not start_date:
        start_date = datetime(2023, 1, 2).date()
    
    end_date = datetime.now().date()
    logging.info(f"Coletando {symbol} - Período: {start_date} até {end_date}")
    
    # Adiciona sufixo .SA para ações brasileiras
    ticker = yf.Ticker(f"{symbol}.SA")
    
    try:
        # Coleta os dados históricos
        df = ticker.history(start=start_date, interval='1d')
        
        # Obtém informações básicas do ativo
        try:
            info = ticker.info
            name = info.get('longName', symbol)
            sector = info.get('sector', 'N/A')
            industry = info.get('industry', 'N/A')
            logging.info(f"Dados do ativo: {symbol} - {name}")
            logging.info(f"Setor: {sector} | Indústria: {industry}")
        except:
            name = symbol  # Se não conseguir obter o nome, usa o símbolo
            logging.warning(f"Não foi possível obter informações adicionais para {symbol}")
        
        if not df.empty:
            logging.info(f"Dados obtidos: {len(df)} registros | Primeiro: {df.index.min().strftime('%Y-%m-%d')} | Último: {df.index.max().strftime('%Y-%m-%d')}")
        
        return name, df
    except Exception as e:
        logging.error(f"Erro ao coletar dados de {symbol}: {str(e)}")
        return None, pd.DataFrame()

def process_symbol(symbol, conn):
    try:
        logging.info(f"\n{'='*50}")
        logging.info(f"Processando {symbol}")
        
        # Verifica se o ativo já existe e pega sua última data
        with conn.cursor() as cur:
            cur.execute("SELECT id, name FROM stocks WHERE symbol = %s", (symbol,))
            result = cur.fetchone()
            if result:
                stock_id, current_name = result
                logging.info(f"Ativo encontrado no banco: ID {stock_id} | Nome: {current_name}")
        
        # Se existe, pega última data de atualização
        start_date = None
        if stock_id:
            last_date = get_last_update_date(conn, stock_id)
            if last_date:
                start_date = last_date + timedelta(days=1)
                if start_date >= datetime.now().date():
                    logging.info(f"Dados de {symbol} já atualizados. Última atualização: {last_date}")
                    logging.info(f"{'='*50}\n")
                    return
                logging.info(f"Última atualização: {last_date} | Buscando dados a partir de: {start_date}")
        
        # Coleta dados
        name, df = collect_stock_data(symbol, start_date)
        if name is None or df.empty:
            logging.warning(f"Sem dados para processar para {symbol}")
            logging.info(f"{'='*50}\n")
            return
        
        # Insere ou atualiza o ativo
        stock_id = insert_stock(conn, symbol, name)
        
        # Insere dados históricos
        rows_inserted = insert_historical_data(conn, stock_id, df)
        
        conn.commit()
        logging.info(f"Dados de {symbol} inseridos com sucesso!")
        logging.info(f"Registros inseridos/atualizados: {rows_inserted}")
        logging.info(f"{'='*50}\n")
        
    except Exception as e:
        conn.rollback()
        logging.error(f"Erro ao processar {symbol}: {str(e)}")
        logging.info(f"{'='*50}\n")

def main():
    # Lista completa de símbolos
    symbols = [
      "AALR3", "ABCB4", "ABEV3", "AERI3", "AESB3", "AGRO3", "ALPA4", "ALOS3", "ALUP11", "AMBP3", "ANIM3", "ARML3", "ARZZ3", "ASAI3", "AURE3", "AZUL4", "B3SA3", "BBAS3", "BBDC3",
      "BBDC4",
      "BBSE3",
      "BEEF3",
      "BHIA3",
      "BLAU3",
      "BMOB3",
      "BPAC11",
      "BPAN4",
      "BRAP4",
      "BRFS3",
      "BRKM5",
      "BRPR3",
      "BRSR6",
      "CAML3",
      "CASH3",
      "CBAV3",
      "CCRO3",
      "CEAB3",
      "CIEL3",
      "CLSA3",
      "CMIG3",
      "CMIG4",
      "CMIN3",
      "COGN3",
      "CPFE3",
      "CPLE6",
      "CRFB3",
      "CSAN3",
      "CSMG3",
      "CSNA3",
      "CURY3",
      "CVCB3",
      "CXSE3",
      "CYRE3",
      "DASA3",
      "DIRR3",
      "DXCO3",
      "ECOR3",
      "EGIE3",
      "ELET3",
      "ELET6",
      "EMBR3",
      "ENAT3",
      "ENEV3",
      "ENGI11",
      "EQTL3",
      "ESPA3",
      "EVEN3",
      "EZTC3",
      "FESA4",
      "FLRY3",
      "FRAS3",
      "GFSA3",
      "GGBR4",
      "GGPS3",
      "GMAT3",
      "GOAU4",
      "GOLL4",
      "GRND3",
      "GUAR3",
      "HAPV3",
      "HBSA3",
      "HYPE3",
      "IFCM3",
      "IGTI11",
      "INTB3",
      "IRBR3",
      "ITSA4",
      "ITUB3",
      "ITUB4",
      "JALL3",
      "JBSS3",
      "JHSF3",
      "KEPL3",
      "KLBN11",
      "LAVV3",
      "LEVE3",
      "LJQQ3",
      "LOGG3",
      "LOGN3",
      "LREN3",
      "LUPA3",
      "LWSA3",
      "MATD3",
      "MBLY3",
      "MDIA3",  
      "MEGA3",
      "MGLU3",
      "MILS3",
      "MLAS3",
      "MOVI3",
      "MRFG3",
      "MRVE3",
      "MULT3",
      "MYPK3",
      "NEOE3",
      "NTCO3",
      "ODPV3",
      "ONCO3",
      "ORVR3",
      "PCAR3",
      "PETR3",
      "PETR4",
      "PETZ3",
      "PGMN3",
      "PLPL3",
      "PNVL3",
      "POMO4",
      "POSI3",
      "PRIO3",
      "PSSA3",
      "PTBL3",
      "QUAL3",
      "RADL3",
      "RAIL3",
      "RAIZ4",
      "RANI3",
      "RAPT4",
      "RDOR3",
      "RECV3",
      "RENT3",
      "ROMI3",
      "RRRP3",
      "SANB11",
      "SAPR11",
      "SBFG3",
      "SBSP3",
      "SEER3",
      "SEQL3",
      "SIMH3",
      "SLCE3",
      "SMFT3",
      "SMTO3",
      "SOMA3",
      "SQIA3",
      "STBP3",
      "SUZB3",
      "TAEE11",
      "TASA4",
      "TEND3",
      "TGMA3",
      "TIMS3",
      "TOTS3",
      "TRIS3",
      "TRPL4",
      "TTEN3",
      "TUPY3",
      "UGPA3",
      "UNIP6",
      "USIM5",
      "VALE3",
      "VAMO3",
      " VBBR3",
      "VIVA3",
      "VIVT3",
      "VLID3",
      "VULC3",
      "WEGE3",
      "WIZC3",
      "YDUQ3",
      "ZAMP3",
    ]
    
    start_time = datetime.now()
    logging.info(f"Iniciando coleta de dados - {start_time}")
    
    try:
        conn = psycopg2.connect(DB_URL)
        
        # Usa ThreadPoolExecutor para processar múltiplos símbolos em paralelo
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(process_symbol, symbol, conn)
                for symbol in symbols
            ]
            concurrent.futures.wait(futures)
        
    except Exception as e:
        logging.error(f"Erro de conexão: {str(e)}")
    
    finally:
        if conn:
            conn.close()
    
    end_time = datetime.now()
    duration = end_time - start_time
    logging.info(f"Coleta finalizada - Duração: {duration}")

if __name__ == "__main__":
    main()