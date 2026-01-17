#!/bin/bash
#Esse script verifica a qualidade do código via gitbash: ./quality_check.sh

# Função para verificar o status de saída do ultimo comando
check_status() {
    if [ $? -ne 0 ]; then
        echo "Erro encontrado. Interrrompedo o script."
        exit 1
    fi
}

# Organiza importações em ordem alfabética
echo "(isort) Organizando importações..."
isort .
check_status
echo -e "\n"

# Formatar código
echo "(black) Formatando código..."
black .
check_status
echo -e "\n"

# Verificar tipos estáticos
echo "(mypy) Verificando tipos estáticos..."
mypy .
check_status
echo -e "\n"

# Analisar qualidade do código
echo "(prospector) Analisando qualidade do código..."
prospector .
check_status
echo -e "\n"

# Executar testes
echo "(pytest) Executando testes..."
pytest .
check_status
echo -e "\n"

# Remover arquivos de cache Python
echo "Removendo arquivos de cache Python..."
find . -type d -name "__pycache__" -exec rm -r {} + 2>/dev/null
find . -type f \( -name "*.pyc" -o -name "*.pyco" \) -delete 2>/dev/null

# Remover caches do mypy e pytest
echo "Removendo caches do mypy e pytest..."
find . -type d \( -name ".mypy_cache" -o -name ".pytest_cache" \) -exec rm -r {} + 2>/dev/null
echo "Arquivos de cache removidos com sucesso."
echo -e "\n"

echo "Verificação de qualidade concluída com sucesso!"