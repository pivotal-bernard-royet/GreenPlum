#!/bin/bash

# Copyright (c) 2007 EntIT Software LLC, a Micro Focus company
#
# Description: This script executes load statements in concurrent vsql threads continuously 
#              for a period of time, and measures the throughput during this time
#              frame.
# Input Parameters:
#   $1 CONC: the number of concurrent vsql threads
#   $2 DURATION: the number of seconds to wait until cancel all of the queries that run
#                concurrently
#   $3 SCHEMA: the schema of the tables created for loading data
#
# Output: The total number of load statements that are executed successfully and the workload distribution.
#  
 
# The first input parameter specify the number of concurrent vsql threads.
if [ -z "$1" ]; then
  echo "Execute load statements in 10 concurrent vsql threads by default"
  CONC=10
else
  CONC=$1
fi

# The second input parameter specify the duration of the concurrent executions in seconds.
if [ -z "$2" ]; then
  echo "Execute load statements in $CONC concurrent vsql sessions for 60 seconds by default"
  DURATION=60
else
  DURATION=$2
fi

if [ -z "$3" ]; then
  echo "Create tables for loading data under schema ETS_LOAD by default"
  SCHEMA=ETS_LOAD
else
  SCHEMA=$3
fi

# Create tables for loading data
vsql -c "create schema if not exists ${SCHEMA}"
table_name_prefix=ETS_LOAD

for i in $(seq 1 "$CONC")
do
  echo "Create Table ${table_name_prefix}_${i}"
  vsql -c "create table if not exists ${SCHEMA}.${table_name_prefix}_${i} ( a int, b int, c int, d int, e int, f int); truncate table ${SCHEMA}.${table_name_prefix}_${i};"
done

# Get the node addresses which are used by distributing the workload.
node_array=( $(vsql -q -t -c "select node_address from nodes") )
nodenum=${#node_array[@]}

# Generate data file for load on each node
for node in "${node_array[@]}"
do
  echo "Generate data file on $node"
  ssh $node "perl -e 'for (\$j=0;\$j<1000000;++\$j){printf(\"%d|%d|%d|%d|%d|%d\n\", \$j, \$j*20, \$j*40, \$j*60, \$j*80, \$j*100);}' > /home/dbadmin/data_50m"
done

# Alternatively the data file can be located on a S3 bucket.
#perl -e 'for ($j=0;$j<1000000;++$j){printf("%d|%d|%d|%d|%d|%d\n", $j, $j*20, $j*40, $j*60, $j*80, $j*100);}' > /home/dbadmin/data_50m
# Specify a S3 bucket in the aws cp command below.
#bucket=
#aws s3 cp /home/dbadmin/data_50m s3://${bucket}/data_50m
#
# Uncomment these 2 lines if load from S3
#aws_id=`vsql -c "select get_config_parameter('AWSAuth');" | head -n 3 | tail -n 1 | cut -d: -f1`
#aws_key=`vsql -c "select get_config_parameter('AWSAuth');" | head -n 3 | tail -n 1 | cut -d: -f2`

# Clear data collector
#vsql -c "select clear_data_collector();"
vsql -c "select clear_data_collector('RequestsCompleted');"
vsql -c "select clear_data_collector('QueryExecutions');"
vsql -c "select clear_data_collector('RequestsIssued');"

# Keep running copy statements in concurrent vsql threads.
for x in $(seq 1 "$CONC")
# Load data from data files located on instance store, this command need to be commented out when load from data files on S3
do yes "copy ${SCHEMA}.${table_name_prefix}_${x} from '/home/dbadmin/data_50m';" | vsql -h ${node_array[$(((x-1)%nodenum))]} > /dev/null &  
# Provide a S3 bucket to load data directly from S3
# do (echo "SELECT AWS_SET_CONFIG('aws_id', '${aws_id}');SELECT AWS_SET_CONFIG('aws_secret', '${aws_key}');" ; yes "copy ${SCHEMA}.${table_name_prefix}_${x} WITH SOURCE S3(url='s3://${bucket}/data_50m') direct;") | vsql -h ${node_array[$(((x-1)%nodenum))]} > /dev/null &  
done

# Let the load statements run for a period of time and cancel all sessions.
sleep "$DURATION"

vsql -c "select close_all_sessions();"

# Get the workload distribution during this period of time.
echo "Workload distribution during $DURATION seconds."
vsql -c "select dqe.node_name, count(*) from dc_query_executions dqe where dqe.execution_step='Plan' and exists (select 1 from dc_requests_issued dri where dri.transaction_id = dqe.transaction_id and dri.statement_id=dqe.statement_id and dri.request_type='LOAD' )  group by node_name order by node_name;"

# Count the number of load statements that are completed successfully during this period of time.
echo "COPY statements completed successfully during $DURATION seconds."
vsql -c "select count(*) from dc_requests_issued dri,dc_requests_completed drc where dri.request_type='LOAD' and drc.success='t' and dri.session_id=drc.session_id and dri.request_id=drc.request_id;"
