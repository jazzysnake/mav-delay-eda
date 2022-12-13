# MAV delay exploratory data analysis

*MAV is the state owned hungarian railway company, notorious for delays. The software in this
repository aims to find the causes and show the trends in the delays.
Made for the Big Data course of Budapest University of Technology and Economics*

## Data origin

Two data sources were used in the analysis:

    - Delay cause data
    - Train location and delay data

The first source was obtained by scraping the public MAV webpage starting from 2022.10.05.
The second source was given to us by u/gaborauth on reddit, who made a great
interactive [webpage](https://mav-stat.info/)
on the same topic. We are thankful for his efforts.

## Dataset description

### Delay cause data

This dataset contains data from 2022.10.05. sampled every 2 minutes.

| name         | type | meaning                                                 | :key:              |
|--------------|------|---------------------------------------------------------|--------------------|
| id           | int  | Record's unique id                                      | :heavy_check_mark: |
| timestamp    | long | Record's timestamp                                      | :x:                |
| elvira_id    | text | Unique id of a departed train used value until arriving | :x:                |
| relation     | text | Starting and end stations                               | :x:                |
| train_number | text | The train's unique id                                   | :x:                |
| lat          | real | Latitude                                                | :x:                |
| lon          | real | Longitude                                               | :x:                |
| line_kind    | text | Service provider's name, e.g. MAV, HEV                  | :x:                |
| line         | text | Railway line id, only used for foreign ones             | :x:                |
| delay        | real | Delay in minutes                                        | :x:                |
| delay_cause  | text | Delay's cause                                           | :x:                |

### Train location and delay data

This dataset contains data from 2021.01.01 to 2022.10.10, sampled every minute.

| name        | type     | meaning                                                 | :key:              |
|-------------|----------|---------------------------------------------------------|--------------------|
| epoch       | int      | Elapsed days since 1970.01.01.                          | :heavy_check_mark: |
| relation    | text     | Starting and end stations                               | :heavy_check_mark: |
| trainnumber | text     | The train's unique id                                   | :heavy_check_mark: |
| created     | timeuuid | The record's unique id                                  | :heavy_check_mark: |
| delay       | int      | Delay in minutes                                        | :x:                |
| elviraid    | text     | Unique id of a departed train used value until arriving | :x:                |
| lat         | float    | Latitude                                                | :x:                |
| line        | text     | Railway line id, only used for foreign ones             | :x:                |
| lon         | float    | Longitude                                               | :x:                |
| servicename | text     | Service provider's name, e.g. MAV, HEV                  | :x:                |
