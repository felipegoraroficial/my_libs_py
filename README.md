# servify

Biblioteca Python para leitura de dados (xlsx/csv/json/parquet/delta/tabelas) em
ambientes **Databricks**, com detecção automática de encoding/delimitador/JSON
multiline e logging integrado.

## Instalação

```bash
pip install servify
```

## Uso rápido

```python
import servify as sf

df = sf.read_data(
    "/Volumes/meu_catalogo/meu_schema/volume/arquivo.csv", "csv")
```

Para arquivos XLSX, o schema é opcional. Quando não informado, a lib usa a
primeira linha como cabeçalho e infere automaticamente os tipos das colunas:

```python
df = sf.read_data(
    "/Volumes/meu_catalogo/meu_schema/volume/arquivos_xlsx/", "xlsx")
```

## Configuração (opcional)

Antes de chamar `sf.read_data(...)`, o usuário pode ajustar as opções abaixo
diretamente em `sf.<opção> = valor`:

```python
import servify as sf

sf.show_options()  # lista todas as opções disponíveis e o valor atual de cada uma

sf.LOG_ENABLED = True                    # exibe logs no terminal (default: False)
sf.PERSIST_LOGS_CATALOG = "meu_catalogo" # catalog do Unity Catalog p/ persistir logs
sf.PERSIST_LOGS_SCHEMA = "meu_schema"    # schema do Unity Catalog p/ persistir logs
sf.PERSIST_LOGS = True                   # persiste logs em tabela Delta (default: False)
sf.PERSIST_LOG_MIN_LEVEL = "ERROR"       # nível mínimo persistido (default: "WARNING")

df = sf.read_data(
    "/Volumes/meu_catalogo/meu_schema/volume/arquivo.csv", "csv")
```

| Opção                    | Tipo | Default     | Observação |
|---------------------------|------|-------------|------------|
| `LOG_ENABLED`            | bool | `False`     | Exibe logs no terminal. |
| `PERSIST_LOGS`           | bool | `False`     | Persiste logs em tabela Delta do Unity Catalog. |
| `PERSIST_LOGS_CATALOG`   | str  | `None`      | Necessário antes de `PERSIST_LOGS = True`. |
| `PERSIST_LOGS_SCHEMA`    | str  | `None`      | Necessário antes de `PERSIST_LOGS = True`. |
| `PERSIST_LOG_MIN_LEVEL`  | str  | `"WARNING"` | `DEBUG`, `INFO`, `WARNING`, `ERROR` ou `CRITICAL`. |

### O que acontece se `catalog` / `schema` não forem informados?

Se `sf.PERSIST_LOGS = True` for definido **sem** `PERSIST_LOGS_CATALOG` e
`PERSIST_LOGS_SCHEMA` já preenchidos, a lib **não levanta exceção**: ela imprime
um aviso (`[WARN] persist_logs não habilitado: ...`) e mantém `PERSIST_LOGS`
como `False` até que ambos sejam definidos. Ou seja, o comportamento padrão
(sem persistência) continua funcionando normalmente; a persistência em Delta é
sempre opt-in e só é ativada quando `catalog` e `schema` estão presentes.

```python
import servify as sf

sf.PERSIST_LOGS = True

# [WARN] persist_logs não habilitado: informe 'catalog' e 'schema' do Unity
# Catalog (ex.: sf.PERSIST_LOGS_CATALOG e sf.PERSIST_LOGS_SCHEMA) antes de
# sf.PERSIST_LOGS = True.

sf.PERSIST_LOGS_CATALOG = "meu_catalogo"
sf.PERSIST_LOGS_SCHEMA = "meu_schema"

sf.PERSIST_LOGS = True  # agora habilita normalmente
```

## Licença

MIT