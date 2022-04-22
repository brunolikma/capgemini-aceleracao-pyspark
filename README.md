# Capgemini - Aceleração PySpark 2022

Este projeto é parte do Programa de Aceleração [PySpark](https://spark.apache.org) da [Capgemini Brasil](https://www.capgemini.com/br-pt).
[<img src="https://www.capgemini.com/wp-content/themes/capgemini-komposite/assets/images/logo.svg" align="right" width="140">](https://www.capgemini.com/br-pt)

## Sobre

Este projeto consiste em realizar tarefas que buscam garantir a qualidade dos dados para responder perguntas de negócio a fim de gerar relatórios de forma assertiva. As tarefas são essencialmente apontar inconsistências nos dados originais, e realizar transformações que permitam tratar as inconsistências e enriquecer os dados. Em resumo, o projeto está organizado em três módulos: (1) qualidade, (2) transformação, e (3) relatório.

## Dependências

Para executar os Jupyter Notebooks deste repositório é necessário ter o [Spark instalado localmente](https://spark.apache.org/downloads.html) e também as seguintes dependências:

`pip install pyspark findspark`

## Estrutura de diretórios

```
├── Notebooks
│   ├── 1_quality.ipynb          <- Contém apontamentos de dados inconsistêntes.
│   ├── 2_transformation.ipynb   <- Contem tratamentos dos dados.
│   ├── 3_report.ipynb           <- Contém respostas de negócio baseadas em dados.
|
├── data                    <- Diretório contendo os dados brutos.
|   ├── census-income
|        ├── census-income.csv              <- Dados brutos
|        ├── census-income.name             <- Informações das Colunas
|   ├── communities-crime
|         ├── communities-crime.csv         <- Dados brutos
|         ├── communities-crime.name        <- Informações das Colunas
|   ├── online-retail
|         ├── online-retail.csv             <- Dados brutos
|         ├── online-retail.name            <- Informações das Colunas
│   ├── airports.csv      <- Dados Brutos
│   ├── planes.csv        <- Dados Brutos
│   ├── flights.csv       <- Dados Brutos
│
├── scripts
|   ├── README.md
|   ├── census-income.py                     <- Scripts do dados census-income.csv
|   ├── communities-crime.py                 <- Scripts do dados communities-crime.csv
|   ├── online-retail.py                     <- Scripts do dados online-retail.csv
|
├── .gitignore
├── LICENSE
├── README.md
```
