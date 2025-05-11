from pyspark.sql.functions import col, when, regexp_replace, udf, to_date, max, regexp_extract, approx_count_distinct, count
from pyspark.sql.types import StringType,DoubleType
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
import re


class reading_data():

    def last_partition_delta(nome_tabela, coluna_particao):
        """
        Lê a última partição de uma tabela Delta registrada no catálogo usando Spark SQL.

        Args:
            nome_tabela (str): O nome completo da tabela Delta no catálogo
                                (ex: 'nome_do_banco_de_dados.nome_da_tabela').
            coluna_particao (str): O nome da coluna de particionamento.

        Returns:
            pyspark.sql.DataFrame: Um DataFrame contendo os dados da última partição.
                                    Retorna um DataFrame vazio se a tabela não existir
                                    ou se não houver partições.
        """

        spark = SparkSession.builder.getOrCreate()
        
        try:
            df = spark.table(nome_tabela)
        except Exception as e:
            print(f"Erro ao acessar a tabela '{nome_tabela}': {e}")
            return spark.createDataFrame([], schema=df.schema if 'df' in locals() else [])

        ultima_particao_df = df.select(max(coluna_particao).alias("ultima_particao"))
        ultima_particao = ultima_particao_df.first()["ultima_particao"] if ultima_particao_df.first() else None

        if ultima_particao is not None:
            filtro = f"{coluna_particao} = '{ultima_particao}'"
            df_ultima_particao = df.where(filtro)
            print(f"Tabela '{nome_tabela}' filtrada pela última partição: {ultima_particao}")

            qtd = df_ultima_particao.count()

            assert qtd > 0, f"A última partição '{ultima_particao}' da tabela '{nome_tabela}' está vazia."
            
            print(f"Leitura da tabela '{nome_tabela}' carregada com sucesso. Número de linhas: {qtd}")

            return df_ultima_particao
        else:
            print(f"Não foram encontradas partições na tabela '{nome_tabela}'.")
            return spark.createDataFrame([], schema=df.schema)


class silver_data():

    def union_dfs_list(dataframe_list):

        print("Verificando se lista de dataframes está vazia")
        if not dataframe_list:
            return None

        print("Verificando se lista de dataframes contém apenas 1 df")
        if len(dataframe_list) == 1:
            return dataframe_list[0]
        
        print(f"Quantidade de dataframes na lista: {len(dataframe_list)}")

        print("Iniciando processo para unir dataframes")
        df_final = dataframe_list[0]
        for i in range(1, len(dataframe_list)):
            df_final = df_final.union(dataframe_list[i])

        linhas_final = df_final.count()

        print(f"União entre os dataframes realizado, quantidade de linhas: {linhas_final}")
            
        total_linhas = sum(df.count() for df in dataframe_list)

        assert total_linhas == linhas_final, f"União dos datraframes falhou!"

        return df_final


    def filter_not_null_value(df, coluna):
        """
        Filtra um DataFrame para remover valores nulos em uma coluna específica.

        :param df: DataFrame do PySpark
        :param coluna: Nome da coluna onde será aplicado o filtro
        :return: DataFrame filtrado sem valores nulos na coluna especificada
        """

        dffiltered = df.filter(col(coluna).isNotNull())

        qtddotal = df.count()
        qtdnotnull = df.filter(col(coluna).isNotNull()).count()
        qtdnull = df.filter(col(coluna).isNull()).count()

        print(f"dataframe filtrado, numero de linhas: {qtdnotnull}")

        assert qtddotal == (qtdnull + qtdnotnull)

        print(f"Filtro ralizado com sucesso")
        print(f"df origem {qtddotal} linhas = df filtrado {qtdnotnull} linhas + df não filtrado {qtdnull} linhas")

        return dffiltered


    def define_data_columns(df):
        """
        Identifica colunas do tipo string, verifica se todos os valores seguem o formato 'YYYY-MM-DD' com regex,
        e converte as colunas para o tipo date quando todas as ocorrências forem compatíveis.

        :param df: DataFrame do PySpark
        :return: DataFrame com as colunas convertidas para o formato date quando aplicável
        """
        formato_regex = r"^\d{4}-\d{2}-\d{2}$"  # Regex para formato 'YYYY-MM-DD'

        colunas_string = [coluna for coluna, dtype in df.dtypes if dtype == "string"]  

        print(f"Colunas strings identificadas no dataframes: {colunas_string}")

        for coluna in colunas_string:
            df_sem_nulos = df.filter(col(coluna).isNotNull())

            match_count = df_sem_nulos.filter(regexp_extract(col(coluna), formato_regex, 0) != "").count()
            total_count = df_sem_nulos.count()

            if match_count == total_count:  

                print(f"Coluna com padrões de data para a conversão: {coluna}")

                df = df.withColumn(coluna, to_date(col(coluna), "yyyy-MM-dd"))

                novo_tipo = dict(df.dtypes)[coluna]
                assert novo_tipo == "date", f"Erro: A coluna {coluna} não foi convertida corretamente! Tipo atual: {novo_tipo}"

                print(f"Coluna {coluna} convertida com sucesso, tipo identificado = {novo_tipo}")

        return df


    def define_numeric_columns(df):
        # Regex para identificar valores percentuais e monetários
        regex_percentual = re.compile(r"^\d+%$")
        regex_monetario = re.compile(r"^R\$?\s?\d{1,3}(\.\d{3})*(,\d{2})?$")

        # Obtendo colunas de tipo string
        colunas_string = [coluna for coluna, dtype in df.dtypes if dtype == "string"]
        colunas_percentuais = []
        colunas_monetarias = []

        # Identifica colunas com valores percentuais e monetários
        for coluna in colunas_string:
            df_sem_nulos = df.filter(col(coluna).isNotNull())
            valores_amostra = df_sem_nulos.select(coluna).rdd.map(lambda row: row[0]).collect()

            if any(bool(regex_percentual.match(str(valor))) for valor in valores_amostra):
                print(f"A coluna '{coluna}' contém valores no formato percentual.")
                colunas_percentuais.append(coluna)

            if any(bool(regex_monetario.match(str(valor))) for valor in valores_amostra):
                print(f"A coluna '{coluna}' contém valores no formato monetário.")
                colunas_monetarias.append(coluna)

        # Aplica a conversão para valores percentuais
        for coluna in colunas_percentuais:
            df = df.withColumn(
                coluna,
                when(
                    col(coluna).rlike("^\d+%$"),
                    (regexp_replace(col(coluna), "%", "").cast(DoubleType()) / 100)
                ).otherwise(col(coluna))
            ).withColumn(coluna, col(coluna).cast(DoubleType()))

            print(f"Coluna {coluna} convertida com sucesso para tipo 'double'.")

        # Aplica a conversão para valores monetários
        for coluna in colunas_monetarias:
            df = df.withColumn(
                coluna,
                when(
                    col(coluna).rlike("^R\\$?\\s?\\d{1,3}(\\.\\d{3})*(,\\d{2})?$"),
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(col(coluna), "R\\$", ""), 
                            "\\.", "" 
                        ),
                        ",", "."  
                    ).cast(DoubleType())
                ).otherwise(col(coluna))
            ).withColumn(coluna, col(coluna).cast(DoubleType()))

            print(f"Coluna {coluna} convertida com sucesso para tipo 'double'.")

        return df


class gold_data():
    

    def extract_memory(df, column_name):

        """
        Adiciona uma coluna com a quantidade de memória em GB extraída de outra coluna do DataFrame.

        Parâmetros:
        df (DataFrame): O DataFrame original.
        column_name (str): O nome da coluna que contém a informação de memória.

        Retorna:
        DataFrame: O DataFrame com a nova coluna 'memoria'.
        """
        
        def extract_memory_info(info):

            """
            Extrai a quantidade de memória em GB de uma string fornecida.

            Parâmetros:
            info (str): A string contendo a informação de memória.

            Retorna:
            str: A quantidade de memória em GB encontrada na string ou '-' se não encontrada.
            """

            if isinstance(info, str) and info:
                padrao = r'(\d+)\s*(G[Bb])'
                resultado = re.search(padrao, info, re.IGNORECASE)
                if resultado:
                    return resultado.group(0)
            return '-'

        extrair_memoria_udf = udf(extract_memory_info, StringType())
        return df.withColumn('memoria', extrair_memoria_udf(col(column_name)))
    

    def extract_characters(df, col_name, col_extract, padrao):
        
        """
        Extrai caracteres específicos de uma coluna e coloca o resultado em outra coluna do DataFrame.

        :param df: DataFrame do PySpark.
        :param col_name: Nome da coluna onde o resultado da extração será armazenado.
        :param col_extract: Nome da coluna da qual os caracteres serão extraídos.
        :param padrao: O padrão de regex usado para extrair os caracteres.
        :return: DataFrame atualizado.
        """
        
        df = df.withColumn(col_name, regexp_extract(col(col_extract), padrao, 1))
        
        return df


    def condition_like(df, new_column_name, condition_column, pattern):
        
        """
        Adiciona uma nova coluna ao DataFrame com valores 'Sim' ou 'Nao' 
        com base em uma condição de correspondência de padrão.

        Parâmetros:
        df (DataFrame): O DataFrame de entrada.
        new_column_name (str): O nome da nova coluna a ser adicionada.
        condition_column (str): O nome da coluna existente a ser verificada.
        pattern (str): O padrão regex a ser correspondido na coluna condition_column.

        Retorna:
        DataFrame: O DataFrame com a nova coluna adicionada.
        """
        
        df = df.withColumn(new_column_name, when(col(condition_column).rlike(pattern), 'Sim').otherwise('Nao'))

        return df


class writing_data():

    def last_partition_delta(df, path, modo):

        melhor_coluna = None
        menor_cardinalidade_alta = float('inf')
        total_linhas = df.count()

        for coluna in df.columns:
            # Calcula a cardinalidade aproximada para grandes DataFrames
            cardinalidade = df.agg(approx_count_distinct(col(coluna)).alias("cardinalidade")).collect()[0]["cardinalidade"]

            # Critério 1: Evitar colunas com cardinalidade muito alta
            if cardinalidade > total_linhas * 0.5:  # Limite arbitrário, pode ser ajustado
                print(f"Coluna '{coluna}' tem alta cardinalidade ({cardinalidade}), não recomendada para particionamento.")
                continue

            # Critério 2: Preferir colunas com cardinalidade moderada a baixa
            # e que distribuam os dados de forma razoavelmente uniforme (avaliação aproximada)
            if cardinalidade < menor_cardinalidade_alta:
                # Avaliação da distribuição aproximada
                frequencias = df.groupBy(coluna).agg(count("*").alias("count")).withColumn("proporcao", col("count") / total_linhas).collect()
                distribuicao_ok = True
                for row in frequencias:
                    if row["proporcao"] < 0.01:  # Evitar partições muito pequenas (limite arbitrário)
                        distribuicao_ok = False
                        break

                if distribuicao_ok:
                    menor_cardinalidade_alta = cardinalidade
                    melhor_coluna = coluna

        if melhor_coluna:
            print(f"Coluna sugerida para particionamento: '{melhor_coluna}' (cardinalidade aproximada: {menor_cardinalidade_alta}).")
            print(f"Iniciando o carregamento dos daddos no caminho {path} e com o modo {modo}")

            # Salva o DataFrame Spark no formato delta
            df.write.format("delta") \
                .partitionBy(melhor_coluna) \
                .mode(modo) \
                .saveAsTable(path)
            
            # Validação: Verificar se os dados foram salvos corretamente
            print("Validando os dados salvos...")

            try:
                delta_table = DeltaTable.forPath(df.sparkSession, path)
                df_validacao = delta_table.toDF()
                total_linhas_salvas = df_validacao.count()

                if total_linhas_salvas == total_linhas:
                    print(f"Validação concluída: Todos os {total_linhas} registros foram corretamente salvos na tabela Delta.")
                else:
                    print(f"Alerta: Apenas {total_linhas_salvas} de {total_linhas} registros foram encontrados na tabela Delta.")
            except Exception as e:
                print(f"Erro na validação dos dados: {e}")

        else:
            print("Nenhuma coluna ideal encontrada para particionamento com base nos critérios definidos.")
            print("Considere outras estratégias como clustering líquido ou revise os critérios de análise.")
 