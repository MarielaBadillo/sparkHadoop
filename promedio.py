import urllib2
import json
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

conf = SparkConf().setAppName("Temp").setMaster("local")
sc = SparkContext(conf=conf)
url = "http://144.202.34.148:8000/obtenerTemperatura"
post_params = '{"Tipo": "Completa","NoCo":"08590312"}'
response = urllib2.urlopen(url, post_params)
json_response = json.loads(response.read())
D = json_response["D"]
RDD = sc.parallelize(D)
promedio = (RDD.map(lambda x: x["Temp"]).reduce(lambda x, y: float(x) + float(y)))/(RDD.count())
print("El promedio de temperatura es de: "+str(promedio)+" grados")
