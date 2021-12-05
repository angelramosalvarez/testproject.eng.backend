# Objective
Create a streaming data processing pipeline which can test for anomalous data in real-time.

# Context
Anomaly detection is an important part of data processing, bad quality data will lead to bad quality insights. As such the first part of our data processing does some basic anomaly detection.  
As part of this test project you will create a simple anomaly detection pipeline in Apache Flink using the API it provides. The pipeline will read data from the provided files, do stream processing to allocate an anomalous score, and then write the data into InfluxDB.  
Both the original values and the anomalous score should be written to InfluxDB for each sensor reading.

The following dataset can be used for this project: https://www.dropbox.com/s/3ww0xoitwkzaate/TestFile.zip?dl=0  
It is also included in the resources folder of the project.

# Anomaly Detection Method
There are libraries which provide anomaly detection functionality, however many don’t work well for streaming data. The following algorithm can be used to give a score:  

For a sliding window of values (100 values should give ok results)  
Calculate the interquartile range (IQR) for the array  
Based on the IQR, score the value being processed with the following:  
If the value is < 1.5 * IQR, assign 0  
If it is >= 1.5 * IQR and < 3 * IQR, assign 0.5  
If it is >= 3 * IQR, assign 1

# Constraints
The project should be provided in a Git repository such as on gitlab.com  
It is expected it will be in Java using Maven to build the project  
InfluxDB is available as a Docker image  
Instructions on how to run the project should be provided

# Requirements
In order to run the project you need to have installed in your computer:
- Maven
- Docker
- Apache Flink 
- Git
- Java
    
# Run project
1. Create FatJar 

    1.2. Clone the project from gitlab
    
    ```sh
    git clone git@gitlab.com:angelramosalvarez/testproject.eng.backend.git
    ```
    
    1.3. Execute maven command line, the fatjar project will be create in target directory into [your project directory]
    
    ```sh
    cd testproject.eng.backend
    mvn clean package
    ```
2. Execute docker InfluxDB instance:

    ```sh
    docker run -d --name influxdb -p 8086:8086 \
        -e INFLUXDB_DB=anomaly_detection \
        -e INFLUXDB_ADMIN_USER=admin \
        -e INFLUXDB_ADMIN_PASSWORD=admin \
        -v influxdb:/var/lib/influxdb influxdb:1.8.3
    ```
     
3. Deploy FatJar project in Flink local cluster    

    3.1. Start Flink Cluster
    
    ```sh
    [your flink instalation]/bin/start-cluster.sh
    ```
    
    3.2. Deploy FatJar in Flink cluster
    
    ```sh
    flink run --detached target/sensors-anomaly-detection-shaded-1.0-SNAPSHOT.jar --configFile config.local.yaml --csvPath target/classes/sensor-data.csv
    ```
    
4. See result in Influx datadase

    4.1. Go into docker influxdb container
    
    ```sh
    docker exec -it influxdb bash
    ```
    
    4.2. Execute influx command line 
    
    ```sh
    influx
    ```
    
    4.3. Use database anomaly_detection
    
    ```sh
    use anomaly_detection;
    ```
    
    4.4. Select data points (this command show up the result of the project)
    
    ```sh
    select * from datapoints
    ```


