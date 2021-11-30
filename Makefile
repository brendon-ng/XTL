build:
	mvn clean package

<<<<<<< HEAD
run:
=======
run: build
>>>>>>> 8d7a6bd... OOP Class Structure and Build (#6)
	java -cp target/XTL-1.0.jar app.App resources/config.json

clean:
	rm -fr data
	rm dependency-reduced-pom.xml
	mvn clean
