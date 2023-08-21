# HistoricEvents

This is a repo for the following problem statement

You have been given (on Moodle) a sample dataset of historical events, and a Spark program that implements a group by and count on a data set of historical events.
You can use this program as a template for the questions below.

Write a spark program that implements the following four queries as separate functions, and a main program that takes as argument an integer from 1 to 4, and executes the relevant query from the below:

     Given an entity like Gandhi, Greece, etc. create a dataset of [entity, category1, category 2, count] considering events where entity occurs in the description
    As above, given a file with entities, one per line.   Assume the entities are given in a file called entities.
    Find the count of all instances in each  year (consider only events with granularity year)
    For each year, find the rank of the year if sorted in descending order by number of events in the year.

In all cases the result should be computed using spark and output to console as in the template program.

