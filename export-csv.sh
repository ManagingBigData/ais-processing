#!/bin/bash
# Originally a one-liner, but we added some comments for readability. :)
# As such, doesn't take parameters, needs to be configured by editing.

# Create folder on local FS for output
mkdir -p ~/ais_data
rm ~/ais_data/*

# Get data from Hadoop and convert to CSV
echo {00000..5} | xargs -P 0 -n 1 sh -c 'hadoop fs -cat /data/aisOutput_2/part-r-$0 | awk '"'"'{ print strftime("%FT%TZ",$1),$2,$3,$4 }'"'"' | tr -s '"'"' '"'"' '"'"','"'"' > ~/ais_data/$0.csv'

# Combine individual CSV files
cat ~/ais_data/{00000..5}.csv > ~/ais_data/total.csv

# Create (limited) 
echo timestamp,latitude,longitude,count>~/ais_data/total_limited.csv
shuf -n 5000000 ~/data/total.csv | sort >> ~/ais_data/total_limited.csv
