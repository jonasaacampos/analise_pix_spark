# %% [markdown]
# **Table of contents**<a id='toc0_'></a>    
# - [1 - Preparação do Ambiente de Desenvolvimento](#toc1_1_)    
#     - [Preparação do ambiente](#toc1_1_1_)    
#       - [IDEs utilizada](#toc1_1_1_1_)    
#       - [Criar ambiente virtual](#toc1_1_1_2_)    
#       - [Ativar .venv](#toc1_1_1_3_)    
#   - [2 - Data Undesrtanting](#toc1_2_)    
#     - [Data Schema](#toc1_2_1_)    
#   - [3 - Preparação dos Dados](#toc1_3_)    
#     - [Instalando bibliotecas e componentes necessários](#toc1_3_1_)    
#     - [Funções personalizadas para apoio](#toc1_3_2_)    
#     - [Data Schema](#toc1_3_3_)    
#     - [Ajustando a visualização dos dados](#toc1_3_4_)    
#     - [Sumarização dos dados](#toc1_3_5_)    
#   - [4 - Análise exploratória](#toc1_4_)    
#     - [Insights e Análises iniciais](#toc1_4_1_)    
#   - [5 - Modelagem](#toc1_5_)    
#   - [6 - Modelo de Predição de Fraudes](#toc1_6_)    
# - [Avaliação do Modelo](#toc2_)    
# - [Deployment](#toc3_)    
# 
# <!-- vscode-jupyter-toc-config
# 	numbering=false
# 	anchor=true
# 	flat=false
# 	minLevel=1
# 	maxLevel=6
# 	/vscode-jupyter-toc-config -->
# <!-- THIS CELL WILL BE REPLACED ON TOC UPDATE. DO NOT WRITE YOUR TEXT IN THIS CELL -->

# %% [markdown]
# ## <a id='toc1_1_'></a>[1 - Preparação do Ambiente de Desenvolvimento](#toc0_)

# %% [markdown]
# <details>
# <summary>Preparação do ambiente</summary>
# 
# ### <a id='toc1_1_1_'></a>[Preparação do ambiente](#toc0_)
# 
# #### <a id='toc1_1_1_1_'></a>[IDEs utilizada](#toc0_)
# 
#  - VSCode
# 
# #### <a id='toc1_1_1_2_'></a>[Criar ambiente virtual](#toc0_)
# 
# - Command Pallet (ctrl + shift+ p)
# - Python: Create Environment > Venv > Python Version (3.12)'
# 
# #### <a id='toc1_1_1_3_'></a>[Ativar .venv](#toc0_)
# 
# In VsCode terminal, alterar a política de execução de scripts para ativar o ambiente virtual.
# 
# ```bash
# Set-ExecutionPolicy Unrestricted -Scope Process
# 
# # ativar ambiente virtual
# .\.venv\Scripts\activate
# 
# ```
# 
# </details>

# %% [markdown]
# ## <a id='toc1_2_'></a>[2 - Data Undesrtanting](#toc0_)
# 
# Primeiramente, devemos entender tudo sobre a fonte dos dados
# - Como o dado chega até nós?
# - Qual formato virá? 
# - Aonde o processamento será executado (AWS EMR, Cluster On-Premise)? 
# - De quanto em quanto tempo eu preciso gerar esse relatório (mensal, diário, near-real time)?
# 

# %% [markdown]
# Os dados foram compartilhados via `*.json`. Saber como os dados serão ingeridos são de vital importância para delimitar a forma como lidaremos com nosso projeto. Análises em tempo real (streaming) são diferentes de análises em lotes (bacthes). Análises pontuais como esta também adotam uma estratégia diferentes das que requerem análises periódicas.
# 
# ### <a id='toc1_2_1_'></a>[Data Schema](#toc0_)
# 
# ```json
# {
#   "id_transacao": inteiro,
#   "valor": texto,
#   "remetente": {
#       "nome": texto,
#       "banco": texto,
#       "tipo": texto
#   }, 
#   "destinatario": {
#       "nome": texto, 
#       "banco":texto,
#       "tipo": texto
#   },        
#   "categoria": texto,
#   "transaction_date":texto,
#   "chave_pix":texto,
#   "fraude":inteiro,
# }
# ```

# %% [markdown]
# ## <a id='toc1_3_'></a>[3 - Preparação dos Dados](#toc0_)
# 
# Agora é hora de começar a preparar os dados de acordo com as necessidades do escopo de trabalho.

# %% [markdown]
# ### <a id='toc1_3_1_'></a>[Instalando bibliotecas e componentes necessários](#toc0_)

# %%
#%pip install pyspark
#%pip freeze > requirements.txt

# %%
import os
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    length,
    max,
    min,
    avg,
    format_number,
    count,
    lit,
    floor,
    udf,
    round,
    when,
    year,
    month,
    dayofmonth,
    dayofweek,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression

# %% [markdown]
# ### <a id='toc1_3_2_'></a>[Funções personalizadas para apoio](#toc0_)

# %%
# função para inicializar o spark
def spark_initialize_session(app_name = 'My Analysis'):
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .config('spark.ui.port', '4050')
        .appName(app_name)
        .getOrCreate()
    )

    return spark


# função para retornar a quantidade de linhas e colunas de um dataframe spark
def spark_show_info_df(df):
    df_lines = df.count()
    df_columns = len(df.columns)
    print(f'Dados do dataframe atual\nLinhas: {df_lines:,} | Colunas: {df_columns}')

# %%
df_path = f'data{os.sep}raw{os.sep}case_final.json'
spark = spark_initialize_session()

# %% [markdown]
# **não usar o json formatado, isso causa lentidão e erros no algoritmo**

# %% [markdown]
# <details>
# <summary>json schema anotations</summary>
# 
# ### <a id='toc1_3_3_'></a>[Data Schema](#toc0_)
# 
# ```json
# {
#   "id_transacao": inteiro,
#   "valor": texto,
#   "remetente": {
#       "nome": texto,
#       "banco": texto,
#       "tipo": texto
#   }, 
#   "destinatario": {
#       "nome": texto, 
#       "banco":texto,
#       "tipo": texto
#   },        
#   "categoria": texto,
#   "transaction_date":texto,
#   "chave_pix":texto,
#   "fraude":inteiro,
# }
# ```
# 
# </details>

# %%
# definição do data schema
# amostra dos dados
# { "id_transacao": 100999, "valor": 7058.09, "remetente": { "nome": "Jonathan Gonsalves", "banco": "BTG", "tipo": "PF" }, "destinatario": { "nome": "Lais Nascimento", "banco": "Nubank", "tipo": "PF" }, "chave_pix": "aleatoria", "categoria": "vestuario", "transaction_date": "2022-02-25 09:31:47", "fraude": 0 }

data_schema_pix_remetente_destinatario = StructType([
    StructField('nome', StringType()),
    StructField('banco', StringType()),
    StructField('tipo', StringType())
    ])

data_schema_pix = StructType([
    StructField('id_transacao', IntegerType()),
    StructField('valor', DoubleType()),
    StructField('remetente', data_schema_pix_remetente_destinatario),   
    StructField('destinatario', data_schema_pix_remetente_destinatario),
    StructField('categoria', StringType()),
    StructField('transaction_date', StringType()),
    StructField('chave_pix', StringType(), True),
    StructField('fraude', IntegerType(), True),
    ])

# %%
df_path = f'data{os.sep}raw{os.sep}case_final.json'
spark = spark_initialize_session()

# %%
# "transaction_date": "2022-02-25 09:31:47"
df = spark.read.json(df_path, 
                     schema=data_schema_pix, 
                     timestampFormat='yyyy-MM-dd HH:mm:ss')

# %%
# verificar tipo dos dados
df.printSchema()

# %%
# visualiar os dados no dataframe
df.show(5)

# %% [markdown]
# *precisamos remover as estruturas aninhadas que estão nas colunas remetente e destinatário*
# 
# Para que cada coluna tenha apenas um tipo de dado, precisamos fazer o achatamento dos nossos dados.

# %%
df_flatten = df.withColumns({
    'remetente_nome': col('remetente').getField('nome'),
    'remetente_banco': col('remetente').getField('banco'),
    'remetente_tipo': col('remetente').getField('tipo'),

    'destinatario_nome': col('destinatario').getField('nome'),
    'destinatario_banco': col('destinatario').getField('banco'),
    'destinatario_tipo': col('destinatario').getField('tipo'),
    }).drop('remetente', 'destinatario')

# %%
print(df_flatten.printSchema())

# %%
print(df_flatten.show(5))

# %% [markdown]
# ### <a id='toc1_3_4_'></a>[Ajustando a visualização dos dados](#toc0_)

# %%
spark_show_info_df(df_flatten)

# %%
# mostra a quantidade máxima e mínima de caracteres de cada coluna
df_flatten.select([max(length(col(c))).alias(c + '_max_lenght') for c in df_flatten.columns]).show()
df_flatten.select([min(length(col(c))).alias(c + '_min_lenght') for c in df_flatten.columns]).show()

# %%
# ajuste para visualização de todas as colunas
# o padrão é 20, o maior valor possível é 100
# o valor 33 é o suficiente para visualizar todas as colunas com base no nosso dataframe.destinatario_nome_max_lenght
spark.conf.set('spark.sql.debug.maxToStringFields', 33)

# %% [markdown]
# ### <a id='toc1_3_5_'></a>[Sumarização dos dados](#toc0_)

# %%
df_flatten.describe().show()

# %% [markdown]
# ## <a id='toc1_4_'></a>[4 - Análise exploratória](#toc0_)

# %% [markdown]
# ### <a id='toc1_4_1_'></a>[Insights e Análises iniciais](#toc0_)
# 
# **Distribuição de Valores**
# 
# - **Distribuição de Valores de Transação:** A média é R$ 10,303.36, com um desvio padrão de R$ 20,874.99, indicando uma grande variabilidade nos valores das transações.
# - **Transações de Alto Valor:** Os valores podem chegar até R$ 89,996.33, indicando transações de alto valor que podem merecer uma investigação mais detalhada.

# %%
df = df.withColumn("day_of_week", dayofmonth("transaction_date"))
daily_trend = df.groupBy("day_of_week").count().orderBy("day_of_week").toPandas()
plt.figure(figsize=(10, 6))
sns.lineplot(x='day_of_week', y='count', data=daily_trend)
plt.title('Tendência Diária de Transações')
plt.xlabel('Dia do Mês')
plt.ylabel('Contagem')
plt.show()


# %%
df_flatten.describe().show()

# %%
# Converter data
df = df.withColumn("transaction_date", col("transaction_date").cast("timestamp"))

# Adicionar coluna do dia da semana
df = df.withColumn("day_of_week_num", dayofweek("transaction_date"))

# Mapear números para nomes dos dias da semana
df = df.withColumn("day_of_week", when(col("day_of_week_num") == 1, "Dom")
                                  .when(col("day_of_week_num") == 2, "Seg")
                                  .when(col("day_of_week_num") == 3, "Ter")
                                  .when(col("day_of_week_num") == 4, "Qua")
                                  .when(col("day_of_week_num") == 5, "Qui")
                                  .when(col("day_of_week_num") == 6, "Sex")
                                  .when(col("day_of_week_num") == 7, "Sáb"))

# Agrupar e contar por dia da semana
daily_trend = df.groupBy("day_of_week").count().orderBy("day_of_week").toPandas()

# Ordenar dias da semana corretamente
order = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]
daily_trend['day_of_week'] = pd.Categorical(daily_trend['day_of_week'], categories=order, ordered=True)
daily_trend = daily_trend.sort_values("day_of_week")

# Plotar
plt.figure(figsize=(10, 6))
sns.barplot(x='day_of_week', y='count', data=daily_trend, palette='viridis')
plt.title('Tendência Diária de Transações')
plt.xlabel('Dia da Semana')
plt.ylabel('Contagem')
plt.show()

# %% [markdown]
# ## <a id='toc1_5_'></a>[5 - Modelagem](#toc0_)
# 
# - Para qual banco esse cliente mais transfere?
# - Qual é a média de transferências por período que esse cliente faz?
# - Baseando-se no valor das transferências, poderia dar um aumento de crédito?
# - Para o que esse cliente mais usa as transferências?
# - Executar um algoritmo de machine learning que identifique possíveis transações com fraude.
# 

# %%
# Para qual banco foram feitas mais transações?
df_flatten.groupBy('destinatario_banco').count().orderBy(col('count').desc()).show()


# %%
# Quantas transações são realizadas por mês para cada banco?
df_flatten.groupBy(
    date_format('transaction_date', 'yyyy-MM').alias('ano_mes'), 'destinatario_banco'
    ).count().orderBy(col('ano_mes').desc()).show()

# %%
# Valor de transação médio para cada banco
#df_flatten.groupBy('destinatario_banco').avg('valor').orderBy(col('avg(valor)').asc()).show()
average_df = df_flatten.groupBy('destinatario_banco').avg('valor')
formatted_average_df = average_df.withColumn('avg(valor)', format_number(col('avg(valor)'), 2))
formatted_average_df.orderBy(col('avg(valor)').desc()).show()


# %%
# Valor de transação total para cada banco
sum_df = df_flatten.groupBy('destinatario_banco').sum('valor')
formatted_sum_df = sum_df.withColumn('sum(valor)', format_number(col('sum(valor)'), 2))
formatted_sum_df.orderBy(col('sum(valor)').desc()).show()


# %%
# total de transações por mês/banco por categoria
df_flatten.groupBy(
    date_format('transaction_date', 'yyyy-MM').alias('ano_mes'), 'destinatario_banco', 'categoria'
    ).count().orderBy(col('ano_mes').desc()).show()

# %%
# total de transações por banco/categoria
df_flatten.groupBy('destinatario_banco', 'categoria'
    ).count().orderBy(col('categoria').desc()).show()

# %%
# total de transações por categoria/ano
df_flatten.groupBy(
    date_format('transaction_date', 'yyyy').alias('ano'), 'categoria'
    ).count().orderBy(col('ano').desc()).show()

# %%
# total de transações por categoria
df_flatten.groupBy('categoria').count().orderBy(col('count').desc()).show()

# %%
# Quantidade de transações por ano
df_flatten.groupBy(date_format(col("transaction_date"), "yyyy").alias("ano")).agg(
    count("id_transacao").alias("qunt_transacoes")
).orderBy("ano", ascending=False).show()

# %%
# valor total de transações por ano
df_flatten.groupBy(date_format(col("transaction_date"), "yyyy").alias("ano")).sum(
    "valor"
).select("ano", format_number(col("sum(valor)"), 2).alias("total_valor_transacoes")).orderBy(
    "ano", ascending=False
).show()

# %%
# valor médio de transações por ano
df_flatten.groupBy(date_format(col("transaction_date"), "yyyy").alias("ano")).avg(
    "valor"
).select("ano", format_number(col("avg(valor)"), 2).alias("media_transacoes")).orderBy(
    "ano", ascending=False
).show()

# %%
# Quantidade de fraudes
df_flatten.groupBy(col('fraude')).count().alias('quantidade').show()

# %%
df_flatten.printSchema()

# %%
# Quantidade de transações por remetente
df_flatten.groupBy(col('remetente_nome')).count().alias('quantidade').show()

# %%
# Quantidade de transações or tipo de chave pix
df_flatten.groupBy(col('chave_pix')).count().alias('quantidade').show()

# %%
# Quantidade de transações por destinatário
df_flatten.groupBy(col('destinatario_nome')).count().alias('quantidade').orderBy('count', ascending=False).show()

# %%
# Quantidade de fraudes por ano
df_flatten.filter(col("fraude") == 1).groupBy(
    date_format(col("transaction_date"), "yyyy").alias("ano")
).count().select("ano", format_number(col("count"), 2).alias("fraudes_ocorridas")).orderBy(
    "ano", ascending=False
).show()

# %%
#verificar o total de fraudes por ano e tirar a prova dos valores comparados ao método anteriror

# Conta o número de fraudes por ano
df_yearly = df_flatten.filter(col("fraude") == 1).groupBy(
    date_format(col("transaction_date"), "yyyy").alias("ano")
).count().select(
    "ano", format_number(col("count"), 2).alias("total")
).orderBy(
    "ano", ascending=False
)

# Conta o número total de fraudes
df_total = df_flatten.filter(col("fraude") == 1).select(
    lit("Total").alias("ano"), format_number(count("*"), 2).alias("total")
)

# Adiciona a linha total ao DataFrame
df_result = df_yearly.union(df_total)

df_result.show()

# %%
# Agrupamento por cateria e fraude
df_flatten.groupBy('fraude', 'categoria').count().orderBy('fraude', ascending =False).show()

# %%
# faixa de valores em que ocorreram fraudes

df_flatten.filter(col("fraude") == 1).withColumn(
    "range", floor(col("valor") / 1000) * 1000
).groupBy("range").count().orderBy("range").show()

# %%
# Faixa máxima e mínima de valores que ocorreram fraudes
df_flatten.filter(col("fraude") == 1).withColumn(
    "range", floor(col("valor") / 1000) * 1000
).select(
    format_number(max("range"), 0).alias('faixa_max_fraude'),
    min('range').alias('faixa_min_fraude')).show()

# %% [markdown]
# ## <a id='toc1_6_'></a>[6 - Modelo de Predição de Fraudes](#toc0_)

# %%
df_flatten.columns

# %%
df.columns

# %%
indexer = StringIndexer(
    inputCols=[
        "destinatario_nome", 
        "destinatario_banco",
        "destinatario_tipo",
        "categoria",
        "chave_pix"
    ], 
    outputCols=[
        "destinatario_nome_index", 
        "destinatario_banco_index",
        "destinatario_tipo_index",
        "categoria_index",
        "chave_pix_index"
    ])

# %%
df_index = indexer.fit(df_flatten).transform(df_flatten)
df_index.show()

# %%
# para filtros, podemos usar somente colunas numéricas e de data
cols_para_filtrar = [
  "valor",
  "transaction_date",
  "destinatario_nome_index", 
  "destinatario_banco_index",
  "destinatario_tipo_index",
  "chave_pix_index",
  "categoria_index",
  "fraude"
]

# %%
is_fraud = df_index.select(cols_para_filtrar).filter(col("fraude") == 1)
not_fraud = df_index.select(cols_para_filtrar).filter(col("fraude") == 0)


# %%
# separar amostra dos dados de fraude
not_fraud = not_fraud.sample(False, 0.1, 42)

# %%
df_concat = not_fraud.union(is_fraud)
df = df_concat.sort("transaction_date")
df.count()

# %%
train, test = df.randomSplit([0.7, 0.3], seed = 123)
print("train =", train.count(), " test =", test.count())

# %%
is_fraud = udf(lambda fraud: 1.0 if fraud > 0 else 0.0, DoubleType())
train = train.withColumn("is_fraud", is_fraud(train.fraude))

# %%
train.write.mode("overwrite").parquet(f"data{os.sep}stage{os.sep}train.parquet")

# %%
# Create the feature vectors.
assembler = VectorAssembler(
  inputCols = [x for x in train.columns if x not in ["transaction_date", "fraude", "is_fraud"]],
  outputCol = "features")

# Use Logistic Regression.
lr = LogisticRegression().setParams(
    maxIter = 100000,
    labelCol = "is_fraud",
    predictionCol = "prediction")

spark = spark.builder.config("spark.network.timeout", "600s").getOrCreate()

# Repartition the train DataFrame into 4 partitions
# train = train.repartition()

# This will train a logistic regression model on the input data and return a 
# LogisticRegressionModel object which can be used to make predictions on new data.
model = Pipeline(stages = [assembler, lr]).fit(train)

# %%
# Transforma os dados de teste usando o modelo
predicted = model.transform(test)

# Adiciona uma nova coluna 'is_fraud' ao DataFrame
predicted = predicted.withColumn("is_fraud", is_fraud(predicted.fraude))

# Cria uma tabela de contingência (crosstab) entre 'is_fraud' e 'prediction'
crosstab_df = predicted.crosstab("is_fraud", "prediction")

# Converte os nomes das colunas para strings
crosstab_df = crosstab_df.toDF(*(c.replace('.', '_') for c in crosstab_df.columns))

# Calcula a soma total de todas as linhas
total = crosstab_df.select(*(col(c).cast("int") for c in crosstab_df.columns)).rdd.flatMap(lambda x: x).reduce(lambda x, y: x + y)

# Adiciona novas colunas ao DataFrame para mostrar as porcentagens
for column in crosstab_df.columns[1:]:
    crosstab_df = crosstab_df.withColumn(column + '_percent', round((col(column) / lit(total)) * 100, 2))

crosstab_df.show()

# %%
df_flatten.printSchema()

# %% [markdown]
# # <a id='toc2_'></a>[Avaliação do Modelo](#toc0_)
# Será que seu modelo atinge todas as necessidades que foram definidas inicialmente?
# 
# 

# %% [markdown]
# # <a id='toc3_'></a>[Deployment](#toc0_)
# Apresente o relatório com os resultados obtidos.
# 
# 
# 
# 
#   

# %% [markdown]
# 


