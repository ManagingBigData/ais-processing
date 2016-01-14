allAIS = LOAD '/user/s1091859/final/part-r-00000' using JsonLoader('mmsi:chararray,latitude:chararray,longitude:chararray,timestamp:chararray,mmsi2:chararray,shiptype:chararray');

ais = foreach allAIS generate (int)mmsi as mmsi, (float)latitude as latitude, (float)longitude as longitude, (int)timestamp as timestamp, (int)shiptype as shiptype;
ais = FILTER ais BY shiptype>9 AND shiptype <100 AND (shiptype/10==8 OR shiptype/10==9);
g = GROUP ais BY (timestamp-timestamp%3600,(ROUND(latitude*100)/100.0),(ROUND(longitude*100)/100.0));
data = FOREACH g GENERATE FLATTEN($0), COUNT($1);

STORE data INTO 'aisOutput_2';