# -*- coding:utf-8 -*-
import MySQLdb
import ConfigParser

# 读取config.conf配置文件的内容，存入DATA_CONFIG对象中
DATA_CONFIG = ConfigParser.ConfigParser()
DATA_CONFIG.read("config.conf")
config_port = DATA_CONFIG.get("config", "port")


class TransactionManager(object):
	# 连接到数据库，返回一个数据库连接对象
	def __init__(self):
		self.conn = MySQLdb.connect(
			host=DATA_CONFIG.get("db_info", "host"),
			port=int(DATA_CONFIG.get("db_info", "port")),
			user=DATA_CONFIG.get("db_info", "username"),
			passwd=DATA_CONFIG.get("db_info", "password"),
			db=DATA_CONFIG.get("db_info", "db"),
			charset=DATA_CONFIG.get("db_info", "charset"),
			cursorclass=MySQLdb.cursors.DictCursor  # 这里可以强制规定所有的查询结构都已字典而不是元祖的方式来显示，方便取值和处理
		)
		self.cursor = self.conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)

	def startTransaction(self):
		pass

	def commitTransaction(self):
		# 提交事务
		self.cursor.close()
		self.conn.commit()
		self.conn.close()

	def rollbackTransaction(self):
		# 回滚事务
		self.cursor.close()
		self.conn.rollback()
		self.conn.close()

	def db_matchone(self,comm):
		# 创建一个指针来接受并运行传入的msyql命令，并将返回这条语句所影响的行数
		count = self.cursor.execute(comm)
		result = ""
		if count > 0:
			# 使用fetchone方法，从查询结果中获得1条数据，这条数据就是一个字典，可直接取值
			result = self.cursor.fetchone()
		# 返回结果，直接返回一个自己定义的字典
		return {"count": count, "result": result}

	# 查询多条数据的方法，对应于需要返回多条数据的情况，比如查找所有没有维护中的
	def db_matchall(self,comm):
		count = self.cursor.execute(comm)
		result = ""
		if count > 0:
			# 使用fetchall方法，从查询结果中获得所有数据，注意返回结果是一个list，但是list中的每个元素都是一行，是一个字典。是这样子的[{},{},{}]
			result = self.cursor.fetchall()
		return {"count": count, "result": result}

	# 进行不是查询的mysql操作，比如insert和update
	def db_do(self,comm):
		count = self.cursor.execute(comm)
		# 这里cursor.lastrowid返回上一次insert和update操作中所影响到的行的主键，这里一般是id，就算操作多行，也只返回最后一行的id。
		lastid = self.cursor.lastrowid
		# 这里conn.insert_id()返回最后一次insert操作是自增的id的值，即使这个操作不是这一次调用中进行的！！！。
		insertid = int(self.conn.insert_id())
		return {"count": count, "lastid": lastid, "insertid": insertid}  # 这里建议始终调取lastid
