start:
	if [ ! -d "docker-hive" ]; then git clone https://github.com/big-data-europe/docker-hive.git; fi
	cd docker-hive && docker-compose up -d
	cd docker-hive && docker-compose exec hive-server bash /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000

clear:
	cd docker-hive && docker-compose down
