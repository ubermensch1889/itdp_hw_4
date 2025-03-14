## Дано
- Развернутый на 3 нодах кластер HDFS
- Все ноды проименованы (tmpl-nn, tmpl-dn-00, tmpl-dn-00)
- Yarn поднят
- Apache Hive развернут

## Настройка окружения

Устанавливаем нужные инструменты.
`sudo apt install python3-venv`
`sudo apt install python3-pip`

Скачиваем и разархивируем Spark.
`wget https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz`
`tar -xzvf spark-3.5.3-bin-hadoop3.tgz`

Создаем нужные переменные окружения.
```
export HADOOP_CONF_DIR="/home/hadoop/hadoop-3.4.0/etc/hadoop"
export HIVE_HOME="/home/hadoop/apache-hive-4.0.1-bin"
export HIVE_CONF_DIR=$HIVE_HOME/conf
export HIVE_AUX_JARS_PATH=$HIVE_HOME/lib/*
export PATH=$PATH:$HIVE_HOME/bin
export SPARK_LOCAL_IP=<локальный айпи вашей jump-node>
export SPARK_DIST_CLASSPATH="/home/hadoop/spark-3.5.3-bin-hadoop3/jars/*:/home/hadoop/hadoop-3.4.0/etc/hadoop:/home/hadoop/hadoop-3.4.0/share/hadoop/common/lib/*:/home/hadoop/hadoop-3.4.0/share/hadoop/common/*:/home/hadoop/hadoop-3.4.0/share/hadoop/hdfs:/home/hadoop/hadoop-3.4.0/share/hadoop/hdfs/lib/*:/home/hadoop/hadoop-3.4.0/share/hadoop/hdfs/*:/home/hadoop/hadoop-3.4.0/share/hadoop/mapreduce/*:/home/hadoop/hadoop-3.4.0/share/hadoop/yarn:/home/hadoop/hadoop-3.4.0/share/hadoop/yarn/lib/*:/home/hadoop/hadoop-3.4.0/share/hadoop/yarn/*:/home/hadoop/apache-hive-4.0.0-alpha-2-bin/*:/home/hadoop/apache-hive-4.0.0-alpha-2-bin/lib/*"

# переходим в директорию, где лежит Spark
cd spark-3.5.3-bin-hadoop3/
export SPARK_HOME=`pwd`
export PYTHONPATH=$(ZIPS=("$SPARK_HOME"/python/lib/*.zip); IFS=:; echo "${ZIPS[*]}"):$PYTHONPATH
export PATH=$SPARK_HOME/bin:$PATH
```

Переходим на директорию выше, запускаем сервис metastore, создаем и активируем среду.
```
cd ../
python3 -m venv venv
source venv/bin/activate
```

Обновляем pip и устанавливаем ipython.
```
pip install -U pip
pip install ipython
pip install onetl[files]
```


## Подготовка данных

Теперь нам необходимо взять произвольный csv файл, который мы вдальнейшем загрузим на hdfs, преобразуем, как таблицу, и сохраним.

Создаем папку, в которую поместим наш файл.
```
hdfs dfs -mkdir /input
hdfs dfs -chmod g+w /input
```

Загрузим на файл на hdfs.
`hdfs dfs -put <имя вашего файла> /input`

## Работа с Apache Spark

Запускаем консоль ipython и вписываем следующие команды:
```
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from onetl.connection import SparkHDFS
from onetl.connection import Hive
from onetl.file import FileDFReader
from onetl.file.format import CSV
from onetl.db import DBWriter
spark = SparkSession.builder.master("yarn").appName("spark-with-yarn").config("spark.sql.warehouse.dir", "/user/hive/warehouse").config("spark.hive.metastore.uris", "thrift://tmpl-jn:9083").enableHiveSupport().getOrCreate()

hdfs = SparkHDFS(host="tmpl-nn", port=9000, spark=spark, cluster="test")
hdfs.check()
# тут должно вывестись что-то вроде "SparkHDFS(cluster='test', host='tmpl-nn', ipc_port=9000)"

reader = FileDFReader(connection=hdfs, format=CSV(delimiter=",", header=True), source_path="/input")
df = reader.run(["<имя вашего csv файла>"])

# как-нибудь трансформируем наши данные. например, отсортируем
transformed_df = raw_df.orderBy([<названия столбцов, по которым хотим сортировать>], ascending=[<для каждого стобца True, если в порядке возрастания, иначе - False>])

# сохраняем с партицированием
hive = Hive(spark=spark, cluster="test")
writer = DBWriter(
    connection=hive,
    table="test.spark_partitions",
    options={
        "if_exists": "replace_entire_table",
        "partition_by": [<имя столбцов, по которым хотим партицировать>]
    }
)
writer.run(transformed_df)

# выходим
exit
```

Теперь можем проверить получившиеся результаты.
```
hive
USE test;
DESCRIBE FORMATTED spark_partitions;
SELECT * FROM spark_partitions LIMIT 10;
```


