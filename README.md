# Real time anomaly detector

This is a simple code example for the detection of anomalous sensor readings. The data is read from a CSV file, and output into an InfluxDB database.

## Detection 
Each sensor reading is compared to the interquartile range (IQR) for the last 100 values, and scored following a simple logic.

## Output
The output contains the timestamp, the sensor and the score. It's published to an InfluxDB, a time series database.

### Improvements
- Testing: Most relevant parts of the code are not tested. Testing is a fundamental part of code development. It allows for fast development and integration, and gives the developer some security on subsequent changes.

- Run instructions: This project, being a sample project, should have a clear guide on how to run it. This would entail a clear guide on running InfluxDB using Docker.

