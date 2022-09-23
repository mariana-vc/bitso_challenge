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

##### After sign in...
You will find 2 DAGs
- exchanges_processing: for the challenge 1 (“Markets team” needs to track and monitor the activity from all the exchanges that offer the same markets as bitso). 
- spread_monitor: for the challenge 2 (“Markets team” needs to monitor and alert whenever the bid-ask spread from the “order books” in the books btc_mxn and usd_mxn is bigger than 0.1% in Bitso)

After sign in, activate the DAGs and
wait for the process to complete.
