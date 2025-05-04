# Repositório destinado a criação de functions para tratativas de dados em Arquitetura Medalhão!

## Conceito:

A arquitetura medalhão descreve uma série de camadas de dados que denotam a qualidade dos dados armazenados no lakehouse.

Essa arquitetura garante atomicidade, consistência, isolamento e durabilidade à medida que os dados passam por várias camadas 
de validações e transformações antes de serem armazenados em uma disposição otimizada para uma análise eficiente.

Os termos bronze (bruto), prata (validado) e ouro (enriquecido) descrevem a qualidade dos dados em cada uma dessas camadas.

A arquitetura medalhão é um padrão de design de dados usado para organizar dados logicamente. Seu objetivo é melhorar de forma
incremental e progressiva a estrutura e a qualidade dos dados à medida que eles fluem por cada 
camada da arquitetura (de Bronze ⇒ Prata ⇒ Ouro). 

Com o avanço dos dados por essas camadas, as organizações podem melhorar gradativamente a qualidade e a confiabilidade dos dados,
tornando-os mais adequados para aplicativos de Business Intelligence e aprendizado de máquina.

fonte: 
https://docs.databricks.com/aws/pt/lakehouse/medallion

## Funções da Camada Silver

Para utilização das funções abaixo. é necessário utilizar um dataframe em pysaprk

- column_to_date: Converte uma coluna de string para o tipo de dado de data.
- column_to_timestamp: Converte uma coluna de string para o tipo timestamp.
- numbers_to_date: Converte uma coluna de números em datas.
- change_null_numeric: Substitui valores nulos em colunas numéricas por 0.
- change_null_string: Substitui valores nulos em colunas de string por '-'.
- remove_extra_spaces: Remove espaços em branco extras de todas as colunas de string em um DataFrame.
- upper_string_column: Converte todos os caracteres de uma coluna de string para maiúsculas.
- lower_string_column: Converte todos os caracteres de uma coluna de string para minúsculas.
- change_column_name: Altera o nome de uma coluna em um DataFrame.
- union_dataframes: Une uma lista de DataFrames .
- filter_like: Filtra os registros de um DataFrame onde os valores de uma coluna específica correspondem a um padrão regex.
- filter_by_max_date: Filtra o DataFrame para manter apenas as linhas com a maior data.
- organize_data: Ordena o dataframe de acordo com uma coluna de identificação eliminando possivéis duplicatas.

## Funções da Camada Gold

Para utilização das funções abaixo. é necessário utilizar um dataframe em pysaprk

- convert_currency_column: Converte uma coluna de moeda no DataFrame para o tipo double.
- type_monetary: Identifica o tipo de 'moeda' com base de uma coluna específicas.
- extract_memory: Adiciona uma coluna com a quantidade de memória em GB extraída de outra coluna do DataFrame.
- concat_columns: Concatena duas colunas de um DataFrame com um separador "_".
- replace_characters: Substitui um caracter específico por outro em uma coluna do DataFrame.
- extract_characters: Extrai caracteres específicos de uma coluna e coloca o resultado em outra coluna do DataFrame.
- condition_like: Adiciona uma nova coluna ao DataFrame com valores 'Sim' ou 'Nao' com base em uma condição de correspondência de padrão.

## Funções de Teste Funcional

- df_not_empty: Verifica se o Dataframe não está vazio retornando o nnúmero de linhas.
- schema_equals_df_schema: Verifica se o schema corresponde ao que está ao dataframe (Utilizado após a aplicação do Schema ao df).
- count_df_filtered_filter: Verifica se não ocorreu perda de linhas em um filtro de dados.
- count_df_filtered_is_not_null: Verifica a quantidade de linhas nulas e não nulas são iguais ao dataframe original.
- count_union_df: Verifica a consistência da união de um conjunto de DataFrames.
- list_names_equal_df_names: Verifica se os nomes das colunas de um DataFrame (df) são exatamente iguais aos nomes presentes em uma lista (list_name).
- number_columns_list_names_and_df: Verifica se o dataframe possuí a mesma quantidade de nomes em uma lista.