#!/bin/bash

# Copyright (c) 2007 EntIT Software LLC, a Micro Focus company
#
# Description: This script executes queries in concurrent vsql threads continuously 
#              for a period of time, and measures the query throughput during this time
#              frame.
# Input Parameters:
#   $1 CONC: the number of concurrent vsql threads
#   $2 DURATION: the number of seconds to wait until cancel all of the queries that run
#                concurrently
#
# Output: The total number of queries that are executed successfully and the workload distribution.
#  
 
# The first input parameter specify the number of concurrent vsql threads.
if [ -z "$1" ]; then
  echo "Execute queries in 10 concurrent vsql threads by default"
  CONC=10
else
  CONC=$1
fi

# The second input parameter specify the duration of the concurrent executions in seconds.
if [ -z "$2" ]; then
  echo "Execute queries in $CONC concurrent vsql sessions for 60 seconds by default"
  DURATION=60
else
  DURATION=$2
fi

# Provide a lable to identify the queries that are executed by this script.
TAG='example'

# Get the node addresses which are used by distributing the workload.
node_array=( $(vsql -q -t -c "select node_address from nodes") )
nodenum=${#node_array[@]}

# Clear data collector
#vsql -c "select clear_data_collector();"
vsql -c "select clear_data_collector('RequestsCompleted');"
vsql -c "select clear_data_collector('QueryExecutions');"
vsql -c "select clear_data_collector('RequestsIssued');"

# Keep running queries (using a vmart query as an example) in concurrent vsql threads.
for x in $(seq 1 "$CONC" )
  do yes "select /*+label($TAG)*/ fat_content FROM (
          SELECT DISTINCT fat_content
          FROM product_dimension
          WHERE department_description
          IN ('Dairy') ) AS food
          ORDER BY fat_content
          LIMIT 5;" | vsql -h ${node_array[$(((x-1)%nodenum))]} > /dev/null &  
  done

# Let the queries run for a period of time and cancel all sessions.
sleep "$DURATION"

vsql -c "select close_all_sessions();"

# Get the workload distribution during this period of time.
echo "Workload distribution during $DURATION seconds."
vsql -c "select dqe.node_name, count(*) from dc_query_executions dqe where dqe.execution_step='Plan' and exists (select 1 from dc_requests_issued dri where dri.transaction_id = dqe.transaction_id and dri.statement_id=dqe.statement_id and dri.label='$TAG')  group by node_name order by node_name;"

# Count the number of queries that are completed successfully during this period of time.
echo "Queries completed successfully during $DURATION seconds."
vsql -c "select count(*) from dc_requests_issued dri,dc_requests_completed drc where dri.label = '$TAG' and drc.success='t' and dri.session_id=drc.session_id and dri.request_id=drc.request_id;"
