#!/bin/bash

# Vai para o diretório do projeto
cd "$(dirname "$0")/.."

# Ativa o ambiente virtual (se estiver usando)
source scripts/venv/bin/activate

# Atualiza o yfinance para a versão mais recente
pip install --upgrade yfinance

# Executa o script
python3 scripts/collect_history.py 