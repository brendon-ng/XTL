build:
	mvn clean package

run: 
	rm -fr data
	java -cp target/XTL-1.0.jar app.App $(filter-out $@,$(MAKECMDGOALS))

clean:
	rm -fr data
	rm dependency-reduced-pom.xml
	mvn clean
