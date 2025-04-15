import requests
import os
import sys
from datetime import datetime, timedelta
import pytz
import textwrap
from dotenv import load_dotenv

# Carrega variáveis de ambiente
load_dotenv()

# Configuração
REPO_OWNER = os.getenv('GITHUB_OWNER', 'Luc2000')
REPO_NAME = os.getenv('GITHUB_REPO', 'trades-statistic-python')
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')

# Cores para saída
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    END = '\033[0m'

def get_workflows():
    """Obter todos os workflows do repositório"""
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/actions/workflows"
    
    headers = {}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"
    
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"{Colors.RED}Erro ao obter workflows: {response.status_code}{Colors.END}")
        print(response.text)
        sys.exit(1)
    
    return response.json()["workflows"]

def get_workflow_runs(workflow_id, limit=10):
    """Obter execuções recentes de um workflow específico"""
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/actions/workflows/{workflow_id}/runs?per_page={limit}"
    
    headers = {}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"
    
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"{Colors.RED}Erro ao obter execuções do workflow: {response.status_code}{Colors.END}")
        print(response.text)
        return []
    
    return response.json()["workflow_runs"]

def format_duration(started_at, completed_at):
    """Formatar a duração da execução"""
    if not started_at or not completed_at:
        return "N/A"
    
    start = datetime.fromisoformat(started_at.replace('Z', '+00:00'))
    end = datetime.fromisoformat(completed_at.replace('Z', '+00:00'))
    
    duration = end - start
    minutes = duration.seconds // 60
    seconds = duration.seconds % 60
    
    return f"{minutes}min {seconds}s"

def get_status_color(status, conclusion):
    """Retorna a cor correspondente ao status"""
    if status != "completed":
        return Colors.YELLOW
    
    if conclusion == "success":
        return Colors.GREEN
    elif conclusion == "skipped":
        return Colors.BLUE
    else:
        return Colors.RED

def print_workflow_info(workflow):
    """Exibir informações de um workflow"""
    print(f"\n{Colors.BOLD}{workflow['name']} ({workflow['path']}){Colors.END}")
    print(f"Estado: {workflow['state']}")
    
    # Obter execuções recentes
    runs = get_workflow_runs(workflow['id'])
    
    if not runs:
        print(f"{Colors.YELLOW}Nenhuma execução encontrada{Colors.END}")
        return
    
    print(f"\nÚltimas {len(runs)} execuções:")
    
    for run in runs:
        status_color = get_status_color(run['status'], run['conclusion'])
        
        # Formatar data UTC para o horário de Brasília (UTC-3)
        created_at = datetime.fromisoformat(run['created_at'].replace('Z', '+00:00'))
        br_tz = pytz.timezone('America/Sao_Paulo')
        created_at_br = created_at.astimezone(br_tz)
        
        # Calcular quanto tempo atrás
        now = datetime.now(pytz.utc).astimezone(br_tz)
        time_ago = now - created_at_br
        
        if time_ago.days > 0:
            time_str = f"{time_ago.days} dias atrás"
        elif time_ago.seconds // 3600 > 0:
            time_str = f"{time_ago.seconds // 3600} horas atrás"
        else:
            time_str = f"{time_ago.seconds // 60} minutos atrás"
        
        duration = format_duration(run['run_started_at'], run['updated_at'])
        
        # Status formatado com cor
        status_text = f"{status_color}{run['status']}"
        if run['conclusion']:
            status_text += f" ({run['conclusion']}){Colors.END}"
        else:
            status_text += f"{Colors.END}"
        
        print(f"  • {created_at_br.strftime('%d/%m/%Y %H:%M')} ({time_str})")
        print(f"    Status: {status_text}")
        print(f"    Duração: {duration}")
        print(f"    Evento: {run['event']}")
        print(f"    URL: {run['html_url']}")
        print()

def main():
    """Função principal"""
    if not GITHUB_TOKEN:
        print(f"{Colors.YELLOW}Aviso: Token do GitHub não configurado. Alguns dados podem estar limitados.{Colors.END}")
    
    print(f"{Colors.BOLD}Verificando workflows do repositório {REPO_OWNER}/{REPO_NAME}{Colors.END}")
    
    workflows = get_workflows()
    
    if not workflows:
        print(f"{Colors.RED}Nenhum workflow encontrado{Colors.END}")
        return
    
    for workflow in workflows:
        print_workflow_info(workflow)
    
    print(f"\n{Colors.BOLD}Total de workflows: {len(workflows)}{Colors.END}")

if __name__ == "__main__":
    main() 