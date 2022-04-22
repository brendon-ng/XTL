build:
	mvn clean package

run: 
	rm -fr data
	java -cp target/XTL-1.0.jar app.App $(filter-out $@,$(MAKECMDGOALS))

dag-build:
	rm -fr data
	java -cp target/XTL-1.0.jar app.DAGProcessing $(filter-out $@,$(MAKECMDGOALS))

dag-run:
	rm -fr data
	./dag/execute.sh

clean:
	rm -fr data
	rm dependency-reduced-pom.xml
	mvn clean
