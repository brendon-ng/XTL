build:
	mvn clean package

run: 
	java -cp target/XTL-1.0.jar app.App resources/config.json

clean:
	rm -fr data
	rm dependency-reduced-pom.xml
	mvn clean
