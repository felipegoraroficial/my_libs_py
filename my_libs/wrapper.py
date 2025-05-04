from pyspark.sql.functions import col, regexp_extract, row_number, concat_ws, when, trim, regexp_replace, udf, lit, upper, lower, to_date, to_timestamp, expr
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType
import re
from pyspark.sql import DataFrame


class silver_data():

    def change_null_string(df):

        """
        Substitui valores nulos em colunas de string por '-'.

        Parâmetros:
        df (DataFrame): O DataFrame de entrada.

        Retorna:
        DataFrame: O DataFrame com valores nulos substituídos por '-'.
        """

        string_columns = [col_name for col_name, data_type in df.dtypes if data_type == 'string']
        df = df.na.fill('-', subset = string_columns)

        return df


    def change_null_numeric(df, type):

        """
        Substitui valores nulos em colunas numéricas por 0.

        Parâmetros:
        df (DataFrame): O DataFrame de entrada.
        type (str): O tipo de dado das colunas numéricas (ex: 'int', 'double').

        Retorna:
        DataFrame: O DataFrame com valores nulos substituídos por 0 nas colunas numéricas especificadas.
        """

        numeric_columns = [col_name for col_name, data_type in df.dtypes if data_type == type]
        
        df = df.na.fill(0, subset=numeric_columns)

        return df
    

    def organize_data(df, column_id, column_order):

        window_spec = Window.partitionBy(column_id).orderBy(col(column_order).desc())

        df = df.withColumn("row_number", row_number().over(window_spec))

        df = df.withColumn("status", when(col("row_number") == 1, "ativo").otherwise("inativo"))
        df = df.drop("row_number")

        return df


    def remove_extra_spaces(df):

        """
        Remove espaços em branco extras de todas as colunas de string em um DataFrame.

        Esta função percorre todas as colunas do tipo string no DataFrame fornecido
        e remove qualquer espaço em branco extra no início, fim e dentro das strings.

        Parâmetros:
        df (pyspark.sql.DataFrame): O DataFrame de entrada.

        Retorna:
        pyspark.sql.DataFrame: Um novo DataFrame com espaços extras removidos de todas as colunas de string.
        
        """

        string_cols = [col_name for col_name, col_type in df.dtypes if col_type == 'string']

        for col_name in string_cols:

            df = df.withColumn(col_name, regexp_replace(trim(col(col_name)), r'\s+', ' '))
            
        return df


    def filter_like(df, coluna, padrao):

        """
        Filtra os registros de um DataFrame onde os valores de uma coluna específica correspondem a um padrão regex.

        Args:
        df (DataFrame): O DataFrame a ser filtrado.
        coluna (str): O nome da coluna a ser filtrada.
        padrao (str): O padrão regex usado para filtrar os valores da coluna.

        Returns:
        DataFrame: Um novo DataFrame contendo apenas os registros que correspondem ao padrão regex.
        """

        df = df.filter(col(coluna).rlike(padrao))

        return df


    def upper_string_column(df,column_name):

        """
        Converte todos os caracteres de uma coluna de string para maiúsculas.

        Parâmetros:
        df (DataFrame): O DataFrame de entrada.
        column_name (str): O nome da coluna que será convertida para maiúsculas.

        Retorna:
        DataFrame: O DataFrame com a coluna especificada convertida para maiúsculas.
        """

        df = df.withColumn(column_name, upper(col(column_name)))

        return df
    

    def lower_string_column(df,column_name):

        """
        Converte todos os caracteres de uma coluna de string para minúsculas.

        Parâmetros:
        df (DataFrame): O DataFrame de entrada.
        column_name (str): O nome da coluna a ser convertida para minúsculas.

        Retorna:
        DataFrame: O DataFrame com a coluna especificada convertida para minúsculas.
        """

        df = df.withColumn(column_name, lower(col(column_name)))

        return df
    

    def change_column_name(df, col_original, col_change):

        """
        Altera o nome de uma coluna em um DataFrame.

        Parâmetros:
        df (DataFrame): O DataFrame no qual a coluna será renomeada.
        col_original (str): O nome original da coluna.
        col_change (str): O novo nome da coluna.

        Retorna:
        DataFrame: O DataFrame com a coluna renomeada.
        """

        df = df.withColumnRenamed(col_original, col_change)
        
        return df


    def column_to_date(df, column_name, format):

        """
        Converte uma coluna de string para o tipo de dado de data.

        Parâmetros:
        df (DataFrame): O DataFrame que contém a coluna a ser convertida.
        column_name (str): O nome da coluna que será convertida para data.
        format (str): O formato da data na coluna de string.

        Retorna:
        DataFrame: O DataFrame com a coluna convertida para o tipo de dado de data.
        """

        df = df.withColumn(column_name, to_date(column_name, format))
        
        return df
    

    def column_to_timestamp(df,column_name,format):

        """
        Converte uma coluna de um DataFrame para o tipo timestamp.

        Parâmetros:
        df (DataFrame): O DataFrame de entrada.
        column_name (str): O nome da coluna a ser convertida.
        format (str): O formato da data/hora da coluna.

        Retorna:
        DataFrame: O DataFrame com a coluna convertida para timestamp.
        """

        df = df.withColumn(column_name, to_timestamp(column_name,format))

        return df


    def numbers_to_date(df, coluna):
        
        """
        Converte uma coluna de números em datas.

        Parâmetros:
        df (DataFrame): O DataFrame de entrada.
        coluna (str): O nome da coluna que contém os números a serem convertidos.

        Retorna:
        DataFrame: O DataFrame com a coluna convertida para datas.
        """

        df = df.withColumn(coluna, col(coluna).cast(IntegerType()))
        
        df = df.withColumn(coluna, to_date(expr(f"date_add('1899-12-30', `{coluna}`)")))
        
        return df


    def filter_by_max_date(df, column_date):

        """
        Filtra o DataFrame para manter apenas as linhas com a data máxima.

        Parâmetros:
        df (DataFrame): O DataFrame de entrada.
        column_date (str): O nome da coluna que contém as datas.

        Retorna:
        DataFrame: Um DataFrame filtrado contendo apenas as linhas com a data máxima.
        """

        max_date = df.select(max(column_date)).collect()[0][0]

        df = df.filter(df[column_date] == max_date)

        return df


    def union_dataframes(dataframes: list[DataFrame]) -> DataFrame:

        """
        Une uma lista de DataFrames .

        Parâmetros:
        dataframes (lista de dataframes).: A lista de DataFrames a serem unidos.

        Retorna:
        DataFrame: Um novo DataFrame resultante da união da lista de dataframes de entrada.
        """

        if not dataframes:
            print("A lista de DataFrames está vazia.")
            return None

        if len(dataframes) == 1:
            return dataframes[0]

        primeiro_df = dataframes[0]
        dataframes_restantes = dataframes[1:]

        dataframe_unido = primeiro_df
        for df in dataframes_restantes:
            dataframe_unido = dataframe_unido.unionByName(df)

        return dataframe_unido

class gold_data():

    def convert_currency_column(df, col_name):

        """
        Converte uma coluna de moeda no DataFrame para o tipo double.

        Parâmetros:
        df (DataFrame): O DataFrame de entrada.
        col_name (str): O nome da coluna que contém os valores de moeda.

        Retorna:
        DataFrame: O DataFrame com a coluna de moeda convertida para double.
        """

        valor_real_pattern = r"R\$?\s*\d{1,3}(\.\d{3})*(,\d{2})?"

        df = df.withColumn(
                col_name,
                when(col(col_name).rlike(valor_real_pattern),
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    col(col_name),
                                    "[^0-9,R\$]", ""
                                ),
                                "R\$", ""
                            ),
                            "\\.", ""
                        ),
                        ",", "."
                    )
                ).otherwise(col(col_name))
            ).withColumn(col_name, col(col_name).cast("double")
        )
        

        return df
    

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
    

    def type_monetary(df: DataFrame, column: str) -> DataFrame:
        
        """
        Atualiza a coluna 'moeda' no DataFrame com base em condições específicas.

        :param df: DataFrame do PySpark.
        :param column: Nome da coluna a ser analisada para identificação da moeda.
        :return: DataFrame atualizado.
        """

        df = df.withColumn(
            "moeda",
            when(
                col(column).contains("R$"),
                lit("R$")
            ).when(
                col(column).rlike(r"[$€£¥]"),
                regexp_extract(col(column), r"([$€£¥])", 1)
            ).otherwise(lit("moeda não identificada"))
        )
        return df


    def concat_columns(df, column1, column2, name_column):

        """
        Concatena duas colunas de um DataFrame com um separador "_".
        
        Parâmetros:
        df (DataFrame): O DataFrame de entrada.
        column1 (str): O nome da primeira coluna a ser concatenada.
        column2 (str): O nome da segunda coluna a ser concatenada.
        name_column (str): O nome da nova coluna resultante da concatenação.
        
        Retorna:
        DataFrame: O DataFrame com a nova coluna concatenada.
        """

        df = df.withColumn(name_column, concat_ws("_", col(column1).cast("string"), col(column2).cast("string")))
        
        return df


    def replace_characters(df, coluna, caracter, substituto):
        
        """
        Substitui um caracter específico por outro em uma coluna do DataFrame.

        :param df: DataFrame do PySpark.
        :param coluna: Nome da coluna onde o caracter deve ser substituído.
        :param caracter: O caracter a ser substituído.
        :param substituto: O caracter substituto.
        :return: DataFrame atualizado.
        """

        df = df.withColumn(coluna, regexp_replace(coluna, caracter, substituto))
        
        return df


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

class test_data():

    def df_not_empty(df):

        is_empty = df.isEmpty()

        print(f'Está vazio? {is_empty}')

        assert is_empty == False

        count_lines_df = df.count()

        print(f'Quantidade de linhas: {count_lines_df}')

        assert count_lines_df != 0 


    def schema_equals_df_schema(df,schema):

        df_columns_list_names = df.schema.fieldNames()

        schema_columns_list_names = schema.fieldNames()

        diferences_array = ['| Nome Dataframe | Nome Schema | Coluna |']

        for i, name in enumerate(df_columns_list_names):

            if name != schema_columns_list_names[i]:

                diferences_array.append(f'| {name} | {schema_columns_list_names[i]} | {i_+ 1} |')

        print('Nomes')

        print(f'Nomes colunas dataframes: \n{df_columns_list_names}')

        print(f'Nomes colunas schema: \n{schema_columns_list_names}')

        if(len(diferences_array) > 1):

            print(f'Diferença ({len(diferences_array) - 1}):')

            for item in diferences_array:

                print(item)

        assert df_columns_list_names == schema_columns_list_names

        df_columns_list_types = [field.dataType for field in df.schema.fields]

        schema_columns_list_types = [field.dataType for field in schema.fields]

        diferences_array = ['| Tipo Dataframe | Tipo Schema | Coluna |']

        for i, _type in enumerate(df_columns_list_types):

            if _type != schema_columns_list_types[i]:

                diferences_array.append(f'| {_type} | {schema_columns_list_types[i]} | {i_+ 1} |')

        print('Tipos')

        print(f'Tipos colunas dataframes: \n{df_columns_list_types}')

        print(f'Tipos colunas schema: \n{schema_columns_list_types}')

        if(len(diferences_array) > 1):

            print(f'Diferença ({len(diferences_array) - 1}):')

            for item in diferences_array:

                print(item)

        assert df_columns_list_types == schema_columns_list_types


    def count_df_filtered_filter(df,filter):

        df_filter = df.filter(filter)

        count_lines_filtered = df_filter.count()

        df_unfilter = df.filter(~filter)

        count_lines_unfiltered = df_unfilter.count()

        count_lines_df = df.count()

        print(f'Quantidade de linhas filtradas: {count_lines_filtered}')

        print(f'Quantidade de linhas não filtradas: {count_lines_unfiltered}')

        print(f'Quantidade de linhas totais: {count_lines_df}')

        print(f'Resultado: {count_lines_filtered + count_lines_unfiltered} = {count_lines_df}')

        assert (count_lines_filtered + count_lines_unfiltered) == count_lines_df


    def count_df_filtered_is_not_null(df,column):

        df_filter = df.filter(col(column).isNotNull())

        count_lines_filtered = df_filter.count()

        df_unfilter = df.filter(col(column).isNull())

        count_lines_unfiltered = df_unfilter.count()

        count_lines_df = df.count()

        print(f'Quantidade de linhas não nulas: {count_lines_filtered}')

        print(f'Quantidade de linhas nulas: {count_lines_unfiltered}')

        print(f'Quantidade de linhas toais: {count_lines_df}')

        print(f'Resultado: {count_lines_filtered + count_lines_unfiltered} = {count_lines_df}')

        assert (count_lines_filtered + count_lines_unfiltered) == count_lines_df


    def count_union_df(df_union, df_list):

        count_lines_df_list = 0

        for df in df_list:
            count_lines_df_list += df.count()

        count_lines_df_union = df_union.count()

        print(f'Quantidade de linhas da lista de Dataframes: {count_lines_df_list}')

        print(f'Quantidade de linhas do Dataframe Resultante: {count_lines_df_union}')

        print(f'Diferença entre lista de dataframes e dataframe resultante: {count_lines_df_list - count_lines_df_union}')

        assert count_lines_df_list == count_lines_df_union


    def list_names_equal_df_names(df,list_name):

        df_columns_list_names = df.schema.fieldNames()

        diferences_array = ['| Nome Dataframe | Nome Lista | Coluna |']

        for i, name in enumerate(df_columns_list_names):

            if name != list_name[i]:

                diferences_array.append(f'| {name} | {list_name[i]} | {i + 1} |')

        print('Nomes')

        print(f'Nomes colunas dataframe:\n{df_columns_list_names}')

        print(f'Lista de nomes:\n{list_name}')

        if(len(diferences_array) > 1):

            print(f'Diferenças ({len(diferences_array) - 1}):')

            for item in diferences_array:

                print(item)

        assert df_columns_list_names == list_name


    def number_columns_list_names_and_df(df,list_names):

        len_columns_df = len(df.schema)

        len_list_names = len(list_names)

        print(f'Número de colunas dataframe: {len_columns_df}')

        print(f'Número de nomes lista> {len_list_names}')

        print(f'Diferença colunas dataframe e nomes lista: {len_columns_df - len_list_names}')

        assert len_columns_df == len_list_names


 