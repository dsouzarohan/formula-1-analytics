# Formula 1 Data Analysis Project

My first data science project that will be based on the greatest motorsport race. Formula 1.

# Introduction

This project will use Kaggle’s [Formula 1 World Championship](https://www.kaggle.com/rohanrao/formula-1-world-championship-1950-2020) Dataset

- I will be using Python as the primary language.
- The data be stored in a MySQL or Postgres database that will be created using a migration script.
- The dataset has been compiled using the [Ergast Developer API](http://ergast.com/mrd/) by the author of the dataset. At the time of creating this project, the dataset contains race data from 1950-2021 (MIND BLOWN 😱).
- For future data, instead of downloading the dataset, I plan to directly use the API to get the latest race data (can also be implemented in Python)
- The data could have been directly also read from the files, but this would mean that every time to run an analysis, we would have to read the files. Instead, we can just store it in a relational database. Also, with having the data in a DB, we can use other data analysis tools like Tableu to run analyses on the data.