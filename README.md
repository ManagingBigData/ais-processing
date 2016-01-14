# Parsing AIS data for visualization of hotspots

This repo contains a number of files for processing of AIS data, created as part of the course
Managing Big Data at University of Twente.

## Available files
`step0.py`: Pre-processing of raw AIS data to a usable JSON format on Hadoop. (Written by Robin Aly)
`step1.pig`: Filtering and combination of AIS data to a simple row format containing ship location, timestamp and ship types.
`step2.pig`: Clustering of ships on location and time.
`merged.pig`: Steps 1 and 2 in a single .pig file.
`timeslots.pig`: Counts the amount of data points per timeslots. Exports a tsv file with rows containing an excel datetime and a count.
`timeslotsraw.pig`: Same as 'timeslots.pig', with the filters from 'merged.pig' disabled.

## Running the scripts
The Pig Latin scripts can be run with Pig by executing the following command:
	`pig <script.pig>`