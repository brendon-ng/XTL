build:
	mvn clean package

run: 
	rm -fr data
	java -cp target/XTL-1.0.jar app.App $(filter-out $@,$(MAKECMDGOALS))

dag-build:
	if [ -d dag ]; \
		then echo "dag directory exists"; \
	else \
		mkdir dag; \
		mkdir dag/run; \
	fi
	java -cp target/XTL-1.0.jar app.DAGProcessing $(filter-out $@,$(MAKECMDGOALS))

dag-run:
	./dag/run/execute.sh

dag-clean:
	rm -fr dag

clean:
	rm -fr data
	rm dependency-reduced-pom.xml
	mvn clean

all-dag: clean dag-clean build dag-build dag-run