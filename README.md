# **Desafio Semantix Data Engineer**

## **Projeto Final de Spark**

Para realização do projeto foi utilizado o banco de dados presente no link [Banco_de _Dados](https://mobileapps.saude.gov.br/esus-vepi/files/unAFkcaNDeXajurGB7LChj8SgQYS2ptm/04bd3419b22b9cc5c6efac2c6528100d_HIST_PAINEL_COVIDBR_06jul2021.rar). O banco de dados possui registro dos dados de COVID19 no Brasil até o período de 06/07/2021.



#### **Atividade a serem realizadas:**

O projeto baseou-se nas seguintes atividades abaixo:

1. Enviar os dados para o hdfs;
2. Otimizar todos os dados do hdfs para uma tabela Hive particionada por município;
3. Criar 3 vizualizações pelo Spark com os dados enviados para o HDFS;
4. Salvar a primeira visualização como tabela Hive;
5. Salvar a segunda visualização com formato parquet e compressão snappy;
6. Salvar a terceira visualização em um tópico no Kafka;
7. Criar a visualização pelo Spark com os dados enviados para o HDFS separados por região;
8. Salvar a visualização do exercício 6 em um tópico no Elastic;
9. Criar um dashboard no Elastic para visualização dos novos dados enviados.

Link para visualizar o desafio completo: [Desafio_Completo](https://github.com/JessicaAndretto/Desafio_Semantix_Data_Engineer/blob/c2ae36ab5bbd9d77f8711f3de570d147bf0e2d26/Projeto_Final_Spark_Desafio.pdf)

## **Desenvolvimento do projeto**

Para o desenvolvimento do projeto foi criado um ambiente de desenvolvimento utilizando o docker no Windows por meio da utilização do Windows Subsystem for Linux (WSL) 2 com a distribuição Linux Ubuntu 22.04.1 LTS.

Arquivo de configuração e inicialização do docker-compose: [Docker-Compose.yml](https://github.com/JessicaAndretto/Desafio_Semantix_Data_Engineer/blob/90b2eea0847a7d2df458137170f4e48a2ed06f99/docker-compose.yml)

**Ecossistema do ambiente (containers):**

- Hive-server
- Hive-metastore
- hive-metastore-postgresql
- Kafkamanager
- Kafka
- Zookeeper
- Datanode
- Namenode
- HBase-master
- Jupyter-Spark
- Elasticsearch-1
- Kibana-1
- Logstash-1

Ambiente do cluster por Fábio Jardim https://github.com/fabiogjardim/bigdata_docker. Foi utilizado um fork do repositório do Fábio Jardim para o ambiente, conforme realizado durante o treinamento.<br/>
Imagens do elastic, kibana e logstash: docker.elastic.co/elasticsearch/elasticsearch:7.9.2, docker.elastic.co/kibana/kibana:7.9.2 e docker.elastic.co/logstash/logstash:7.9.2


**Inicialização e configurações do ambiente:**

- Setar configuração requerida pelo Elastic Search para manter o container estável:

```bash
sudo vm.max_map_count com no mínimo 262144
```

- Inicialização do cluster:

```bash
docker-compose up -d
```

- Configurar o jar do spark para aceitar o formato parquet em tabelas Hive:

```bash
curl -O https://repo1.maven.org/maven2/com/twitter/parquet-hadoop-bundle/1.6.0/parquet-hadoop-bundle-1.6.0.jar
```
```bash
docker cp parquet-hadoop-bundle-1.6.0.jar jupyter-spark:/opt/spark/jars
```

-  Baixar os dados do desafio dentro da pasta /input e descompactar o arquivo:
```bash
sudo curl -O https://mobileapps.saude.gov.br/esus\
-vepi/files/unAFkcaNDeXajurGB7LChj8SgQYS2ptm/04bd3419b22b9cc5c6efac2c6528100d_HIST_PAINEL_COVIDBR_06jul2021.rar
```

```bash
sudo unrar x 04bd3419b22b9cc5c6efac2c6528100d_HIST_PAINEL_COVIDBR_06jul2021
```
