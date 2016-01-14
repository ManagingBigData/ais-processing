allAIS = LOAD '/data/aisUT/*.gz' using JsonLoader('col1:chararray,col2:chararray,col3:chararray,col4:chararray,col5:chararray,col6:chararray,col7:chararray,col8:chararray,col9:chararray,col10:chararray,col11:chararray,col12:chararray,col13:chararray');

SPLIT allAIS INTO posAIStmp IF (col5 MATCHES '^[CRxL]{1}$'), vehAIStmp IF NOT (col5 MATCHES '^[CRxL]{1}$');

posAIS = foreach posAIStmp generate col3 as mmsi, (float)col4 as latitude, (float)col9 as longitude, col10 as timestamp;
posAIS = FILTER posAIS BY SIZE(mmsi)==9 AND latitude>50.5832 AND latitude<53.7292 AND longitude>3.32 AND longitude<7.3938;

vehAIS = foreach vehAIStmp generate col6 as mmsi, col13 as shiptype;
vehAIS = FILTER vehAIS BY SIZE(mmsi)==9;
vehAIS = DISTINCT vehAIS;

joined = JOIN posAIS BY mmsi, vehAIS BY mmsi;
ais = DISTINCT joined;

STORE ais INTO 'aisOutput_1' USING JsonStorage();