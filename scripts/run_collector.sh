#!/bin/bash

# Vai para o diretório do projeto
cd "$(dirname "$0")/.."

# Define timestamp para os logs
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="logs/collection_${TIMESTAMP}.log"

# Cria diretório de logs se não existir
mkdir -p logs

# Ativa o ambiente virtual (se estiver usando)
source scripts/venv/bin/activate

# Atualiza o yfinance para a versão mais recente
echo "Atualizando yfinance para a versão mais recente..."
pip install --upgrade yfinance

# Define timeout em minutos
TIMEOUT_MINUTES=${1:-25}
TIMEOUT_SECONDS=$((TIMEOUT_MINUTES * 60))

echo "Iniciando coleta de dados com timeout de ${TIMEOUT_MINUTES} minutos..."
echo "Log será salvo em: $LOG_FILE"

# Executa o script com timeout
SCRIPT_TIMEOUT=$TIMEOUT_SECONDS python3 scripts/collect_history.py 2>&1 | tee -a "$LOG_FILE"

# Verifica se o script terminou com sucesso
EXIT_CODE=${PIPESTATUS[0]}
if [ $EXIT_CODE -eq 0 ]; then
  echo "Coleta concluída com sucesso!"
else
  echo "Coleta finalizada com erros ou interrompida. Código de saída: $EXIT_CODE"
fi

echo "Log completo disponível em: $LOG_FILE" 