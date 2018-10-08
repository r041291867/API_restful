# database connection data
DB_CONNECTION = {
	"MONGODB_DB": "HR"
	,"MONGODB_HOST": "192.168.2.201"
	,"MYSQL_DATABASE_HOST":"192.168.2.51"
	,"MYSQL_DATABASE_USER":"louise360992"
	,"MYSQL_DATABASE_PASSWORD":"Foxconn88"
	,"MYSQL_DATABASE_DB":"HR"
#	,"MYSQL_DATABASE_CHARSET":"utf-8"
	,"MONGO_HOST":"192.168.2.201"
	,"MONGO_DBNAME":"HR"
	,"MYSQL_HOST":"192.168.2.51"
	,"MYSQL_USER":"louise360992"
	,"MYSQL_PASSWORD":"Foxconn88"
	,"MYSQL_DB":"HR"
}
# Kafka connection data
Kafka_CONNECTION = {
	"Kafka_HOST" :['broker-0.kafka.mesos:54957','broker-1.kafka.mesos:46290','broker-2.kafka.mesos:35873']
	,"Kafka_TryCount":5
}
# flask vars
FLASK_VARS = {
    'SECRET_KEY': '',
}

# flask-jwt vars
FLASK_JWT_VARS = {
    'JWT_AUTH_URL_RULE': '/api/auth',
}

# another third party libs...
PASSLIB = {
    'HASH_ALGORITHM': 'SHA512',
}
