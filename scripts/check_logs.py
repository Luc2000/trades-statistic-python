#!/usr/bin/env python3
import os
import re
import sys
import glob
from datetime import datetime
import argparse

# Cores para output
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    END = '\033[0m'

def parse_log_file(log_file):
    """Analisa um arquivo de log para extrair métricas importantes"""
    metrics = {
        "start_time": None,
        "end_time": None,
        "duration": None,
        "total_stocks": 0,
        "processed_stocks": 0,
        "successful_stocks": 0,
        "failed_stocks": 0,
        "errors": [],
        "timeouts": 0,
        "version": None
    }
    
    current_stock = None
    
    try:
        with open(log_file, 'r') as f:
            lines = f.readlines()
            
            for line in lines:
                # Captura hora de início
                if "Iniciando coleta de dados" in line:
                    match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})', line)
                    if match:
                        metrics["start_time"] = match.group(1)
                
                # Captura versão do yfinance
                if "Versão do yfinance:" in line:
                    match = re.search(r'Versão do yfinance: ([\d\.]+)', line)
                    if match:
                        metrics["version"] = match.group(1)
                
                # Captura hora de término e duração
                if "Coleta finalizada - Duração:" in line:
                    match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})', line)
                    if match:
                        metrics["end_time"] = match.group(1)
                    
                    match = re.search(r'Duração: (\d+:\d+:\d+\.\d+)', line)
                    if match:
                        metrics["duration"] = match.group(1)
                
                # Captura início de processamento de uma ação
                if "Processando " in line:
                    match = re.search(r'Processando (\w+)', line)
                    if match:
                        current_stock = match.group(1)
                        metrics["total_stocks"] += 1
                        metrics["processed_stocks"] += 1
                
                # Captura inserções bem-sucedidas
                if "Dados de " in line and "inseridos com sucesso" in line:
                    metrics["successful_stocks"] += 1
                
                # Captura falhas
                if "Sem dados para processar para " in line:
                    metrics["failed_stocks"] += 1
                
                # Captura erros
                if "ERROR" in line:
                    metrics["errors"].append(line.strip())
                
                # Captura timeouts
                if "timeout" in line.lower() or "tempo limite" in line.lower():
                    metrics["timeouts"] += 1
    
    except Exception as e:
        print(f"Erro ao analisar o arquivo {log_file}: {str(e)}")
    
    return metrics

def format_duration(duration_str):
    """Formata a string de duração para um formato mais legível"""
    if not duration_str:
        return "N/A"
    
    # Formato HH:MM:SS.mmmmmm
    parts = duration_str.split(':')
    if len(parts) == 3:
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds = float(parts[2])
        
        if hours > 0:
            return f"{hours}h {minutes}m {int(seconds)}s"
        elif minutes > 0:
            return f"{minutes}m {int(seconds)}s"
        else:
            return f"{seconds:.2f}s"
    
    return duration_str

def print_metrics(metrics, verbose=False):
    """Imprime as métricas extraídas em um formato legível"""
    print(f"\n{Colors.BOLD}Métricas de Execução:{Colors.END}")
    print(f"- Início: {metrics['start_time']}")
    print(f"- Término: {metrics['end_time'] or 'N/A'}")
    print(f"- Duração: {format_duration(metrics['duration']) or 'N/A'}")
    print(f"- Versão do yfinance: {metrics['version'] or 'N/A'}")
    
    # Adiciona cores com base no desempenho
    success_rate = metrics["successful_stocks"] / metrics["processed_stocks"] * 100 if metrics["processed_stocks"] > 0 else 0
    success_color = Colors.GREEN if success_rate > 90 else Colors.YELLOW if success_rate > 70 else Colors.RED
    
    print(f"\n{Colors.BOLD}Estatísticas:{Colors.END}")
    print(f"- Total de ações: {metrics['total_stocks']}")
    print(f"- Ações processadas: {metrics['processed_stocks']}")
    print(f"- Ações bem-sucedidas: {success_color}{metrics['successful_stocks']} ({success_rate:.1f}%){Colors.END}")
    print(f"- Ações com falha: {Colors.RED if metrics['failed_stocks'] > 0 else Colors.GREEN}{metrics['failed_stocks']}{Colors.END}")
    print(f"- Timeouts detectados: {Colors.RED if metrics['timeouts'] > 0 else Colors.GREEN}{metrics['timeouts']}{Colors.END}")
    
    if verbose and metrics["errors"]:
        print(f"\n{Colors.BOLD}Erros ({len(metrics['errors'])}):{Colors.END}")
        for i, error in enumerate(metrics["errors"][:10]):  # Limita a 10 erros
            print(f"{i+1}. {Colors.RED}{error}{Colors.END}")
        
        if len(metrics["errors"]) > 10:
            print(f"... e mais {len(metrics['errors']) - 10} erros.")

def main():
    parser = argparse.ArgumentParser(description='Analisar logs da coleta de dados de ações')
    parser.add_argument('-f', '--file', help='Arquivo de log específico para analisar')
    parser.add_argument('-l', '--latest', action='store_true', help='Analisar apenas o log mais recente')
    parser.add_argument('-v', '--verbose', action='store_true', help='Mostrar informações detalhadas, incluindo erros')
    args = parser.parse_args()
    
    # Diretório de logs
    log_dir = "logs"
    
    # Cria o diretório se não existir
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Determina qual arquivo analisar
    log_files = []
    
    if args.file:
        # Arquivo específico
        if os.path.exists(args.file):
            log_files = [args.file]
        else:
            print(f"{Colors.RED}Erro: Arquivo {args.file} não encontrado.{Colors.END}")
            sys.exit(1)
    elif args.latest:
        # Apenas o log mais recente
        all_logs = glob.glob(os.path.join(log_dir, "collection_*.log"))
        if all_logs:
            log_files = [max(all_logs, key=os.path.getctime)]
        else:
            print(f"{Colors.YELLOW}Aviso: Nenhum arquivo de log encontrado em {log_dir}.{Colors.END}")
            sys.exit(0)
    else:
        # Todos os logs
        log_files = sorted(glob.glob(os.path.join(log_dir, "collection_*.log")), key=os.path.getctime)
    
    if not log_files:
        print(f"{Colors.YELLOW}Nenhum arquivo de log encontrado.{Colors.END}")
        print(f"Verifique se existem arquivos no diretório {log_dir} ou use a opção -f para especificar um arquivo.")
        sys.exit(0)
    
    # Analisa cada arquivo
    for log_file in log_files:
        print(f"\n{Colors.BOLD}{Colors.BLUE}Análise do arquivo: {log_file}{Colors.END}")
        metrics = parse_log_file(log_file)
        print_metrics(metrics, args.verbose)

if __name__ == "__main__":
    main() 