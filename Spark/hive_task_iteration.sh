#!/bin/bash

# Set the number of iterations
iterations=5

# Run Hive script in a loop
for ((i=1; i<=$iterations; i++)); do
  hive -e "SET hivevar:iteration=$i; SOURCE s3://airlinedelaybucket/scripts/hive_task_iteration.hql;"
done
