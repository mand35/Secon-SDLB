# Template DataLake
## Introduction
This the code used in the Step-By-Step Walk Through Guide to get started with Smart Data Lake

## Run with Docker

TODO

## Run with Maven
1. Set the following environment variable: `HADOOP_HOME=/path/to/hadoop` (see https://github.com/smart-data-lake/smart-data-lake).
1. Change directory to project root.
1. Execute all feeds: `mvn clean verify`

Note: To execute a single example: 
```
 mvn clean package exec:exec -Dexec.executable="java" -Dexec.args="-classpath %classpath io.smartdatalake.app.LocalSmartDataLakeBuilder --feed-sel <regex-pattern> --config <path-to-projectdir>/src/main/resources" -Dexec.workingdir="target"
```
(requires Maven 3.3.1 or later)

## Run in IntelliJ (on Windows)
1. Ensure, that the directory `src/main/resources` is configured as a resource directory in IntelliJ (File - Project Structure - Modules). 
1. Configure and run the following run configuration in IntelliJ IDEA:
    - Main class: `io.smartdatalake.app.LocalSmartDataLakeBuilder`
    - Program arguments: `--feed-sel <regex-feedname-selector> --config $ProjectFileDir$/src/main/resources`
    - Working directory: `/path/to/sdl-examples/target` or just `target`
    - Environment variables: 
        - `HADOOP_HOME=/path/to/hadoop` (see https://github.com/smart-data-lake/smart-data-lake)

## Programmatic Access to Data Objects (e.g. Notebooks)
To programmatically access DataObjects for testing the config or interactive exploration in Notebooks, execute com.sika.tmc.ProgrammaticAccess in IntelliJ.