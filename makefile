
build: App.class

App.class: App.java
	javac App.java

run: App.class
	java -cp .:mssql-jdbc-11.2.0.jre11.jar App

clean:
	rm App.class
