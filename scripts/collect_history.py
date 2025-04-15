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
import time
import random
import requests
from urllib3.exceptions import HTTPError
import signal

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

# Timeout global para garantir que o script termine (25 minutos)
SCRIPT_TIMEOUT = int(os.getenv('SCRIPT_TIMEOUT', 25 * 60))  # 25 minutos em segundos

# Aliases para tickers problemáticos
SYMBOL_ALIASES = {
    "TRPL4": ["TRPL4.SA", "TRPL4F.SA", "ISA.SA", "TRPL3.SA"],
    "VBBR3": ["VBBR3.SA", "BRDT3.SA"],  # Antigo nome da Vibra Energia
    "BRPR3": ["BRPR3.SA", "BRPR11.SA"],
    "SOMA3": ["SOMA3.SA"],
    "SQIA3": ["SQIA3.SA"],
}

# Lista de User-Agents para rotacionar
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36",
]

# Variável global para controlar se o script está rodando ou já atingiu o timeout
script_running = True

def setup_timeout_handler():
    """Configura um handler para o timeout global do script"""
    def timeout_handler(signum, frame):
        global script_running
        script_running = False
        logging.warning(f"Atingido o tempo limite de execução ({SCRIPT_TIMEOUT//60} minutos). Finalizando o script...")
    
    # Registrar o handler apenas em sistemas que suportam SIGALRM (Unix/Linux)
    if hasattr(signal, 'SIGALRM'):
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(SCRIPT_TIMEOUT)
        logging.info(f"Timeout configurado para {SCRIPT_TIMEOUT//60} minutos")

def setup_random_user_agent():
    """Configura um User-Agent aleatório para evitar bloqueios"""
    user_agent = random.choice(USER_AGENTS)
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})
    return session

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

def collect_stock_data(symbol, start_date=None, max_retries=3):
    """
    Coleta dados de um símbolo com retry e backoff exponencial.
    """
    # Define data mínima como 02/01/2023 se não houver data inicial
    if not start_date:
        start_date = datetime(2023, 1, 2).date()
    
    end_date = datetime.now().date()
    logging.info(f"Coletando {symbol} - Período: {start_date} até {end_date}")
    
    # Configura um User-Agent aleatório
    session = setup_random_user_agent()
    
    retry = 0
    delay = 1
    name = symbol
    
    while retry < max_retries and script_running:
        try:
            # Adiciona delay para não sobrecarregar a API
            if retry > 0:
                jitter = random.uniform(0.5, 1.5)  # Adiciona aleatoriedade ao delay
                sleep_time = delay * jitter
                logging.info(f"Tentativa {retry+1} para {symbol} - Aguardando {sleep_time:.2f}s")
                time.sleep(sleep_time)
                delay *= 2  # Backoff exponencial
            
            # Tenta obter os dados sem sufixo
            ticker = yf.Ticker(symbol, session=session)
            df = ticker.history(start=start_date, end=end_date, interval='1d')
            
            # Se não obtiver dados, tenta com sufixo .SA
            if df.empty:
                logging.info(f"Sem dados para {symbol} sem sufixo. Tentando com sufixo .SA")
                ticker_sa = yf.Ticker(f"{symbol}.SA", session=session)
                df = ticker_sa.history(start=start_date, end=end_date, interval='1d')
                ticker = ticker_sa if not df.empty else ticker
            
            # Obtém informações básicas do ativo
            try:
                info = ticker.info
                if info:
                    name = info.get('longName', info.get('shortName', symbol))
                    sector = info.get('sector', 'N/A')
                    industry = info.get('industry', 'N/A')
                    logging.info(f"Dados do ativo: {symbol} - {name}")
                    logging.info(f"Setor: {sector} | Indústria: {industry}")
            except Exception as info_err:
                logging.warning(f"Não foi possível obter informações adicionais para {symbol}: {str(info_err)}")
            
            if not df.empty:
                logging.info(f"Dados obtidos: {len(df)} registros | Primeiro: {df.index.min().strftime('%Y-%m-%d')} | Último: {df.index.max().strftime('%Y-%m-%d')}")
                return name, df
            
            # Se não conseguiu dados, tenta próxima tentativa
            retry += 1
            logging.warning(f"Não foi possível obter dados para {symbol} na tentativa {retry}")
        
        except Exception as e:
            retry += 1
            logging.error(f"Erro ao coletar dados de {symbol} (tentativa {retry}): {str(e)}")
            
            # Se for erro específico de rede, pode tentar novamente
            if isinstance(e, (requests.exceptions.RequestException, HTTPError)):
                continue
    
    logging.error(f"Falha ao obter dados para {symbol} após {max_retries} tentativas")
    return name, pd.DataFrame()

def try_aliases(symbol, start_date=None):
    """
    Tenta coletar dados do símbolo usando seus aliases conhecidos.
    """
    # Primeiro tenta com o símbolo original
    name, df = collect_stock_data(symbol, start_date)
    
    # Se não conseguiu e o símbolo tem aliases, tenta com eles
    if df.empty and symbol in SYMBOL_ALIASES and script_running:
        logging.info(f"Tentando aliases para {symbol}")
        
        for alias in SYMBOL_ALIASES[symbol]:
            if not script_running:
                break
                
            if alias == symbol:
                continue
                
            alias_name, alias_df = collect_stock_data(alias, start_date)
            
            if not alias_df.empty:
                logging.info(f"Dados obtidos com o alias {alias} para {symbol}")
                return alias_name, alias_df
    
    return name, df

def process_symbol(symbol, conn):
    if not script_running:
        logging.warning(f"Pulando processamento de {symbol} devido ao timeout global")
        return
        
    try:
        logging.info(f"\n{'='*50}")
        logging.info(f"Processando {symbol}")
        
        # Remove espaços extras no início/fim, se houver
        symbol = symbol.strip()
        
        # Verifica se o ativo já existe e pega sua última data
        stock_id = None
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
        
        # Coleta dados, tentando aliases se necessário
        name, df = try_aliases(symbol, start_date)
        
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
        import traceback
        logging.error(traceback.format_exc())
        logging.info(f"{'='*50}\n")

def main():
    # Configura o timeout global
    setup_timeout_handler()
    
    # Lista completa de símbolos (corrigida para remover espaço no VBBR3)
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
      "VBBR3",  # Corrigido para remover espaço
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
    logging.info(f"Versão do yfinance: {yf.__version__}")
    
    try:
        # Configura o timeout mais alto para conexões mais lentas
        conn = psycopg2.connect(DB_URL, connect_timeout=30)
        
        # Define o número máximo de workers com base no número de CPUs disponíveis
        max_workers = min(8, os.cpu_count() or 4)
        logging.info(f"Utilizando {max_workers} threads para processamento")
        
        # Processa os símbolos com ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Limita o número de requisições concorrentes para evitar sobrecarga
            batch_size = 10
            for i in range(0, len(symbols), batch_size):
                if not script_running:
                    logging.warning("Interrompendo o processamento de novos lotes devido ao timeout global")
                    break
                    
                batch = symbols[i:i+batch_size]
                logging.info(f"Processando lote de {len(batch)} símbolos ({i+1}-{i+len(batch)} de {len(symbols)})")
                
                futures = [
                    executor.submit(process_symbol, symbol, conn)
                    for symbol in batch
                ]
                concurrent.futures.wait(futures)
                
                # Pequena pausa entre lotes para evitar sobrecarga
                if i + batch_size < len(symbols) and script_running:
                    time.sleep(5)
        
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