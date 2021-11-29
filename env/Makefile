start:
	if [ ! -d "docker-hadoop" ]; then git clone https://github.com/big-data-europe/docker-hadoop; fi
	cd docker-hadoop && docker-compose up -d
	docker exec -it namenode /bin/bash

stop:
	docker stop nodemanager
	docker stop historyserver
	docker stop datanode
	docker stop resourcemanager
	docker stop namenode

clear: stop
	docker rm nodemanager
	docker rm historyserver
	docker rm datanode
	docker rm resourcemanager
	docker rm namenode
	rm -fr docker-hadoop
