#!/usr/bin/env python3
# scripts/collect_history_curl.py - Versão com curl_cffi para evitar rate limits
import pandas as pd
import json
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
import concurrent.futures
import logging
import time
import random
import sys
import signal
from curl_cffi import requests

# Configuração de logging
log_filename = f'stock_collection_curl_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

# Carrega variáveis de ambiente
load_dotenv()

# Configuração do Supabase (via Postgres direto)
DB_URL = os.getenv('DATABASE_URL')

# Timeout global para garantir que o script termine (25 minutos)
SCRIPT_TIMEOUT = int(os.getenv('SCRIPT_TIMEOUT', 25 * 60))  # 25 minutos em segundos

# Configurações para controle de rate limit
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 3))   # Reduzido para 3 (era 8)
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 5))     # Reduzido para 5 (era 10)
BATCH_DELAY = int(os.getenv('BATCH_DELAY', 15))  # Aumentado para 15s (era 5s)
MIN_REQUEST_INTERVAL = 3.0                       # Segundos mínimos entre requisições

# Aliases para tickers problemáticos
SYMBOL_ALIASES = {
    "TRPL4": ["TRPL4.SA", "TRPL4F.SA", "ISA.SA", "TRPL3.SA"],
    "VBBR3": ["VBBR3.SA", "BRDT3.SA"],  # Antigo nome da Vibra Energia
    "BRPR3": ["BRPR3.SA", "BRPR11.SA"],
    "SOMA3": ["SOMA3.SA"],
    "SQIA3": ["SQIA3.SA"],
}

# Variável global para controlar se o script está rodando ou já atingiu o timeout
script_running = True

# Controle de rate limit
last_request_time = time.time()

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

def respect_rate_limit():
    """Garante tempo mínimo entre requisições para respeitar rate limits"""
    global last_request_time
    
    current_time = time.time()
    elapsed = current_time - last_request_time
    
    if elapsed < MIN_REQUEST_INTERVAL:
        wait_time = MIN_REQUEST_INTERVAL - elapsed + random.uniform(0.5, 2.0)  # Jitter para evitar padrões detectáveis
        time.sleep(wait_time)
    
    last_request_time = time.time()

def format_symbol(symbol):
    """
    Formata o símbolo corretamente para o Yahoo Finance
    Adiciona sufixo .SA para ações brasileiras se necessário
    """
    # Se já tem .SA, retorna como está
    if symbol.endswith('.SA'):
        return symbol
    
    # Para símbolos brasileiros típicos (PETR4, VALE3, etc.)
    if len(symbol) == 5 and symbol[4].isdigit():
        return f"{symbol}.SA"
    
    return symbol

def get_last_update_date(conn, stock_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(date) FROM historical_data
            WHERE stock_id = %s
            """,
            (stock_id,)
        )
        result = cur.fetchone()[0]
        return result

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
    if df is None or df.empty:
        return 0
    
    with conn.cursor() as cur:
        data = [
            (stock_id, date.strftime('%Y-%m-%d'), row['open'], row['high'], row['low'], 
             row['close'], row['volume'], row['min_price'], row['max_price'])
            for date, row in df.iterrows()
            if not pd.isna(row['open']) and 
               not pd.isna(row['high']) and 
               not pd.isna(row['low']) and 
               not pd.isna(row['close']) and 
               not pd.isna(row['volume']) and
               row['volume'] > 0 and  # Garante que teve negociação
               row['open'] > 0 and    # Garante que os preços são válidos
               row['high'] > 0 and
               row['low'] > 0 and
               row['close'] > 0
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

def get_stock_history(symbol, start_date=None, end_date=None, retries=3):
    """
    Obtém o histórico de preços diretamente da API do Yahoo Finance
    usando curl_cffi para impersonar um navegador Chrome
    """
    symbol = format_symbol(symbol)
    logging.info(f"Obtendo histórico para {symbol}")
    
    if not start_date:
        start_date = datetime(2023, 1, 1)
    
    if not end_date:
        end_date = datetime.now()
    
    # Converter para timestamp Unix (segundos)
    period1 = int(start_date.timestamp())
    period2 = int(end_date.timestamp())
    
    # URL da API do Yahoo Finance
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    
    # Parâmetros da requisição
    params = {
        "period1": period1,
        "period2": period2,
        "interval": "1d",
        "events": "history",
        "includeAdjustedClose": "true"
    }
    
    # Cabeçalhos para impersonar um navegador
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,pt-BR;q=0.8,pt;q=0.7",
        "Referer": "https://finance.yahoo.com",
        "DNT": "1",  # Do Not Track
    }
    
    for attempt in range(retries):
        if not script_running:
            logging.warning("Script timeout atingido durante obtenção de dados")
            return None
            
        if attempt > 0:
            logging.info(f"Tentativa {attempt+1}/{retries} para {symbol}")
            # Backoff exponencial
            backoff_time = (2 ** attempt) + random.uniform(0, 1)
            time.sleep(backoff_time)
            
        respect_rate_limit()
        
        try:
            # Usando curl_cffi com impersonação de Chrome
            session = requests.Session(impersonate="chrome110")
            response = session.get(url, params=params, headers=headers)
            
            # Verificar status da resposta
            if response.status_code != 200:
                logging.error(f"Erro ao obter dados: HTTP {response.status_code}")
                continue
            
            # Decodificar a resposta JSON
            data = response.json()
            
            # Verificar se há erro na resposta
            if data.get("chart", {}).get("error"):
                error_msg = data["chart"]["error"]
                logging.error(f"Erro retornado pela API: {error_msg}")
                continue
            
            # Verificar se há resultados
            result = data.get("chart", {}).get("result", [])
            if not result or len(result) == 0:
                logging.warning(f"Sem dados para {symbol}")
                return None
            
            # Processar dados
            df = process_yahoo_data(result[0])
            if df is not None and not df.empty:
                return df
                
        except Exception as e:
            logging.error(f"Erro ao obter histórico para {symbol}: {str(e)}")
    
    logging.error(f"Falha ao obter dados para {symbol} após {retries} tentativas")
    return None

def process_yahoo_data(data):
    """
    Processa os dados do Yahoo Finance para um DataFrame pandas
    Inclui todos os campos necessários: date, open, high, low, close, volume, min_price, max_price
    """
    try:
        # Extrair timestamps e converter para datas
        timestamps = data.get("timestamp", [])
        dates = [datetime.fromtimestamp(ts) for ts in timestamps]
        
        # Extrair dados de preços
        quote = data.get("indicators", {}).get("quote", [{}])[0]
        
        # Extrair e verificar dados necessários
        opens = quote.get("open", [])
        highs = quote.get("high", [])
        lows = quote.get("low", [])
        closes = quote.get("close", [])
        volumes = quote.get("volume", [])
        
        # Verificar se temos dados suficientes
        if not (len(dates) > 0 and len(opens) > 0):
            logging.warning("Dados insuficientes retornados pela API")
            return None
        
        # Criar um DataFrame com todos os campos necessários
        # min_price é o preço mais baixo do dia (low)
        # max_price é o preço mais alto do dia (high)
        df = pd.DataFrame({
            'date': dates,
            'open': opens,
            'high': highs,
            'low': lows,
            'close': closes,
            'volume': volumes,
            'min_price': lows,    # Mesmo que low
            'max_price': highs    # Mesmo que high
        })
        
        # Definir date como índice
        df.set_index('date', inplace=True)
        
        # Limpar valores NaN (None)
        df = df.dropna()
        
        return df
        
    except Exception as e:
        logging.error(f"Erro ao processar dados: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return None

def try_aliases(symbol, start_date=None):
    """
    Tenta coletar dados do símbolo usando seus aliases conhecidos.
    """
    symbol_name = symbol  # Nome padrão é o próprio símbolo
    
    # Primeiro tenta com o símbolo original
    df = get_stock_history(symbol, start_date)
    
    # Se não conseguiu e o símbolo tem aliases, tenta com eles
    if (df is None or df.empty) and symbol in SYMBOL_ALIASES and script_running:
        logging.info(f"Tentando aliases para {symbol}")
        
        for alias in SYMBOL_ALIASES[symbol]:
            if not script_running:
                break
                
            if alias == symbol:
                continue
                
            alias_df = get_stock_history(alias, start_date)
            
            if alias_df is not None and not alias_df.empty:
                logging.info(f"Dados obtidos com o alias {alias} para {symbol}")
                return symbol_name, alias_df
    
    return symbol_name, df

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
        
        if name is None or df is None or df.empty:
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
      "VBBR3",
      "VIVA3",
      "VIVT3",
      "VLID3",
      "VULC3",
      "WEGE3",
      "WIZC3",
      "YDUQ3",
      "ZAMP3",
    ]
    
    # Opção para processar apenas um conjunto limitado de símbolos para teste
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        test_symbols = ["PETR4", "VALE3", "EZTC3", "BBDC4", "ITUB4"]
        symbols = test_symbols
        logging.info(f"Modo de teste ativado. Processando apenas: {test_symbols}")
    
    start_time = datetime.now()
    logging.info(f"Iniciando coleta de dados com curl_cffi - {start_time}")
    logging.info(f"Configurações: MAX_WORKERS={MAX_WORKERS}, BATCH_SIZE={BATCH_SIZE}, BATCH_DELAY={BATCH_DELAY}s")
    
    try:
        # Configura o timeout mais alto para conexões mais lentas
        conn = psycopg2.connect(DB_URL, connect_timeout=30)
        
        # Processa os símbolos com ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Limita o número de requisições concorrentes para evitar sobrecarga
            for i in range(0, len(symbols), BATCH_SIZE):
                if not script_running:
                    logging.warning("Interrompendo o processamento de novos lotes devido ao timeout global")
                    break
                    
                batch = symbols[i:i+BATCH_SIZE]
                logging.info(f"Processando lote de {len(batch)} símbolos ({i+1}-{i+len(batch)} de {len(symbols)})")
                
                futures = [
                    executor.submit(process_symbol, symbol, conn)
                    for symbol in batch
                ]
                concurrent.futures.wait(futures)
                
                # Pausa entre lotes para evitar sobrecarga
                if i + BATCH_SIZE < len(symbols) and script_running:
                    pause_time = BATCH_DELAY + random.uniform(1, 5)
                    logging.info(f"Pausando {pause_time:.2f}s entre lotes para evitar rate limits")
                    time.sleep(pause_time)
        
    except Exception as e:
        logging.error(f"Erro de conexão: {str(e)}")
    
    finally:
        if 'conn' in locals() and conn:
            conn.close()
    
    end_time = datetime.now()
    duration = end_time - start_time
    logging.info(f"Coleta finalizada - Duração: {duration}")

if __name__ == "__main__":
    main() 