allAIS = LOAD '/data/aisUT/*.gz' using JsonLoader('col1:chararray,col2:chararray,col3:chararray,col4:chararray,col5:chararray,col6:chararray,col7:chararray,col8:chararray,col9:chararray,col10:chararray,col11:chararray,col12:chararray,col13:chararray');

SPLIT allAIS INTO posAIStmp IF (col5 MATCHES '^[CRxL]{1}$'), vehAIStmp IF NOT (col5 MATCHES '^[CRxL]{1}$');

posAIS = foreach posAIStmp generate col3 as mmsi, (float)col4 as latitude, (float)col9 as longitude, col10 as timestamp;
posAIS = FILTER posAIS BY SIZE(mmsi)==9 AND latitude>50.5832 AND latitude<53.7292 AND longitude>3.32 AND longitude<7.3938;

vehAIS = foreach vehAIStmp generate col6 as mmsi, col13 as shiptype;
vehAIS = FILTER vehAIS BY SIZE(mmsi)==9;
vehAIS = DISTINCT vehAIS;

joined = JOIN posAIS BY mmsi, vehAIS BY mmsi;

ais = FOREACH joined GENERATE (int)$0 as mmsi, (float)$1 as latitude, (float)$2 as longitude, (int)$3 as timestamp, (int)$5 as shiptype;

ais = FILTER ais BY shiptype>9 AND shiptype <100 AND (shiptype/10==8 OR shiptype/10==9);
g = GROUP ais BY (timestamp-timestamp%3600,(ROUND(latitude*100)/100.0),(ROUND(longitude*100)/100.0));
data = FOREACH g GENERATE FLATTEN($0), COUNT($1);

STORE data INTO '/user/s1086057/final1337';