Objectives of the Case Study

Primarily, the case study is meant as a deep-dive into the usage of Spark. As you have seen while working with Spark, its syntax behaves differently from your regular R syntax. One of the major objectives of this case study is gaining familiarity with how an analysis works in SparkR as opposed to base R.
Learning the basic idea behind using functions in SparkR can be transferred to using other libraries like PySpark. If you are in a company where Python is a primary language, you can easily pick up PySpark syntax and use Spark’s processing power.
The actual process of running a model-building command, boils down to a few lines of code. In trying to find inference from data, the most time-consuming step is preparing the data upto the point of model-building. Hence, we’re gearing this case study more towards exploratory analysis.
 
Problem Statement

Big data analytics allows you to analyse data at scale. It has applications in almost every industry in the world. Let’s consider an unconventional application that you wouldn’t ordinarily encounter.
New York City is a thriving metropolis. Just like most other metros that size, one of the biggest problems its citizens face, is parking. The classic combination of a huge number of cars, and a cramped geography is the exact recipe that leads to a huge number of parking tickets.
In an attempt to scientifically analyse this phenomenon, the NYC Police Department has collected data for parking tickets. Out of these, the data files from 2014 to 2017 are publicly available on Kaggle. We will try and perform some exploratory analysis on this data. Spark will allow us to analyse the full files at high speeds, as opposed to taking a series of random samples that will approximate the population.
For the scope of this analysis, we wish to compare phenomenon related to parking tickets over three different years - 2015, 2016, 2017. All the analysis steps mentioned below should be done for 3 different years. Each metric you derive should be compared across the 3 years. Use the Fiscal years as per the files. You can use calendar year if you like - you will not lose any marks for performing the analysis this way.

Note: although the broad goal of any analysis of this type would indeed be better parking and less tickets, we are not looking for recommendations on how to reduce the number of parking tickets - there are no specific points reserved for this.

The purpose of this case study is to conduct an exploratory analysis that helps you understand the data. Since the size of the dataset is large, your queries will take some time to run, and you will need to identify the correct queries quicker. 
The questions given below will guide your analysis.

The data dictionary is available on this page along with the data.

A> Accessing the dataset
The data for this case study has been placed in an HDFS location.  

B> Questions to be answered in the analysis
The following analysis should be performed on RStudio mounted on your AWS cluster, using the SparkR library. Remember, you should do this analysis for all the 3 years, and possibly compare metrics and insights across the years.

C>Examine the data.

    1. Find total number of tickets for each year.
    2. Find out how many unique states the cars which got parking tickets came from.
    3. Some parking tickets don’t have addresses on them, which is cause for concern. Find out how many such tickets there are.

D>Aggregation tasks

    1. How often does each violation code occur? (frequency of violation codes - find the top 5)
    2. How often does each vehicle body type get a parking ticket? How about the vehicle make? (find the top 5 for both)
    3. A precinct is a police station that has a certain zone of the city under its command. Find the (5 highest) frequencies of:
        Violating Precincts (this is the precinct of the zone where the violation occurred). Using this, can you make any insights for parking violations in any specific areas of the city? 
        Issuing Precincts (this is the precinct that issued the ticket)
    4.  Find the violation code frequency across 3 precincts which have issued the most number of tickets - do these precinct zones have an exceptionally high frequency of certain violation codes? Are these codes common across precincts?
        You’d want to find out the properties of parking violations across different times of the day:
    5. The Violation Time field is specified in a strange format. Find a way to make this into a time attribute that you can use to divide into groups.
        Find a way to deal with missing values, if any.
    6. Divide 24 hours into 6 equal discrete bins of time. The intervals you choose are at your discretion. For each of these groups, find the 3 most commonly occurring violations
    7. Now, try another direction. For the 3 most commonly occurring violation codes, find the most common times of day (in terms of the bins from the previous part)
       Let’s try and find some seasonality in this data
       First, divide the year into some number of seasons, and find frequencies of tickets for each season.
       Then, find the 3 most common violations for each of these season
    8. The fines collected from all the parking violation constitute a revenue source for the NYC police department. Let’s take an example of estimating that for the 3 most commonly occurring codes.
        Find total occurrences of the 3 most common violation codes
        Then, search the internet for NYC parking violation code fines. You will find a website (on the nyc.gov URL) that lists these fines. They’re divided into two categories, one for the highest-density locations of the city, the other for the rest of the city. For simplicity, take an average of the two.
        Using this information, find the total amount collected for all of the fines. State the code which has the highest total collection.
    9. What can you intuitively infer from these findings?
