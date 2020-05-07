# Music Recommender System
## Description

In this project, we build a music recommender system model to predict a playlist for each user of the BeepTunes dataset according to their taste and collection of track info.
BeepTunes is the largest digital music store in Iran.

## Project approach

### 1. Popularity based

The most trivial recommendation algorithm is to simply present each song
in descending order of its popularity skipping those songs already consumed by the user, regardless of the user’s taste profile.

### 2. Collaborative Filtering

It can be either user-based or item-based. In user-based recommendation,
users who listen to the same songs in the past tend to have similar interests
and will probably listen to the same songs in future. In the item-based
recommendation strategy, songs that are often listened by the same user
tend to be similar and are more likely to be listened together in future by
some other user.

### 3. Content Based

It calculates the similarity between tracks based on the tags and categories and recommend by checking the cosine similarity of given tracks.

### 4. Hybrid Approach

We used a hybrid approach combining Collaborative Filtering, Content-Based Filtering & Popularity based techniques which shows recommended tracks based on user activity on our database. You can find more information about our hybrid approach in the API files.

## Requirements

Please install the following tools:\
*Sklearn*\
*Scipy*\
*Pandas*\
*Numpy*\
*Hadoop*\
*Spark*\
*MongoDB*\
*Flask*

## Dataset

We put some samples of our dataset on the "dataset" path in this repository. You can check more information about the data preprocessing phase in our presentation:\
https://www.slideshare.net/TadehAlexani/beeptunes-music-recommender-system


## Instructions to run the code

### Collabrative Filtering:

Runing the model by executing the crontab_job.sh file on the colaborative-filtering/scripts path which will export your data on the configured MongoDB. 

### Content Based:

Run the model by inputting a such CSV file as you can find one on dataset folder at the begining of the code.
You can find a sample CSV file on the dataset path (dataset/ContentBasedInputData.xlsx).\
You can check your result with the sample result file on the dataset path. (dataset/contentBasedResult.xlsx)

## Dashboard

![dashboard-screenshot-1](https://user-images.githubusercontent.com/22890731/80225079-d7dfb980-865f-11ea-9026-4dff309cda27.png)

When you implement API on your server and run the code using the above instruction you can access a dashboard like one below:\
[**Go to Dashboard**](https://tadeha.github.io/music-recommender-system/)\
*In the docs/index.html file, replace YOUR_URL with your server url and enjoy making recommendations!*

## Group:
Niloufar Farajpour- [@niloufarfar](https://github.com/niloufarfar/)\
Mohamadreza Kiani- [@mreza-kiani](https://github.com/mreza-kiani/)\
Mohamadreza Fereydooni- [@mohamadreza99](https://github.com/mohamadreza99/)\
Tadeh Alexani- [@tadeha](https://github.com/tadeha/)
