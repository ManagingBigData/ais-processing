allAIS = LOAD '/data/aisUT/*.gz' using JsonLoader('col1:chararray,col2:chararray,col3:chararray,col4:chararray,col5:chararray,col6:chararray,col7:chararray,col8:chararray,col9:chararray,col10:chararray,col11:chararray,col12:chararray,col13:chararray');

posAIStmp = FILTER allAIS BY col5 MATCHES '^[CRxL]{1}$';

posAIS = foreach posAIStmp generate col3 as mmsi, (float)col4 as latitude, (float)col9 as longitude, (long)col10 as timestamp;
posAIS = FILTER posAIS BY SIZE(mmsi)==9 AND latitude>50.5832 AND latitude<53.7292 AND longitude>3.32 AND longitude<7.3938;

g = GROUP posAIS BY (timestamp-timestamp%7200);
data = FOREACH g GENERATE FLATTEN($0), COUNT($1);
data = FOREACH data GENERATE ToString(ToDate($0*1000),'dd-MM-yyyy HH:mm'), $1;
STORE data INTO 'timeslotswithdate';
