# Data Engineer Challenge

### Requirements and Installation

#### Docker Compose Cluster

The repository contains 4 shell scripts that will set up a docker-compose 
cluster in order to build the environment for the challenge. The functions of each shell can be described next:

* start.sh: Docker build of all the containers and set them up and ready to use.
* stop.sh: Stops all the containers without delete any of them
* restart.sh:Stops all the containers and runs them all again
* reset.sh: Deletes all the containers in the machine for a clean start

#### Airflow Connections

##### Sign in Airflow with:
- User: airflow
- Password: airflow

The airflow scheduler needs to add some fields to the 
postgres and the api connections, this has to be done manually due issues
with the airflow CLI.

##### postgres connection
- Conn Type: Postgres
- Password: airflow

##### pronostico_api connection
- Conn Type: HTTP

After setting up the conections, activate the DAG and
wait for the process to complete.
