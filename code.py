import sys
from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

class c_partition():

	def __init__(self):
		pass

	def _StartSparkSession(self):

		self.spark = SparkSession.builder.appName("Drop Partition Table").enableHiveSupport().getOrCreate()
		sys.path.insert(0, SparkFiles.getRootDirectory())

		self.log4jLogger = self.spark._jvm.org.apache.log4j

		return self.spark, self.log4jLogger

	def _MsckRepairTable(self):

		logger = self.log4jLogger.LogManager.getLogger(__name__ + "." + self.__class__.__name__ + "." + sys._getframe().f_code.co_name)

		v_msck_repair = 'MSCK REPAIR TABLE %s' % self.dbtable

		logger.info("Script drop partition")

		self.spark.sql(v_msck_repair)

	def _DropPartition(self):

		logger = self.log4jLogger.LogManager.getLogger(__name__ + "." + self.__class__.__name__ + "." + sys._getframe().f_code.co_name)

		logger.info("Starting drop partition")

		self.MsckRepairTable()

		df = self.spark.sql('show partitions %s' % self.dbtable)

		df.show()

		if df.count() == 0:
			logger.info("Not exists partitions")
			return

		partitions = '\n'.join(df.rdd.map(tuple).map(lambda reg: '|'.join([str(colu) for colu in reg])).collect())

		list = partitions.split("\n")
		my_list = []
		for x in list:
			aux = x.split("/")[0]
			partition = aux.split("=")[0]+"="+'"'+ aux.split("=")[1]+'"'
			if partition not in my_list:
				my_list.append(partition)

		result = "\n".join(x for x in my_list)
		query = "ALTER TABLE " + self.dbtable + "DROP " + result

		logger.info("Script drop partition " + query)

		self.spark.sql(query)

		self._MsckRepairTable()

		logger.info("Finishing drop partition")

	def run(self, database, table):
		self.database = database
		self.table = table
		self.dbtable = "{}.{}".format(self.database, self.table)

		self._StartSparkSession()
		self._DropPartition()