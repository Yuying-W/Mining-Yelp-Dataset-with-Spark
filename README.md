Note: This is a private repository, so `View on GitHub` gives a 404 error.

# Mining-Yelp-Dataset-with-Spark
Big Data Mining using Apache Spark, data source: https://www.yelp.com/dataset

## Contributors
|  <img alt="yuying_avatar" src="imgs/yuying_avatar.jpg" width="100"/>                           |  <img alt="yang_avatar" src="imgs/yang_avatar.png" width="100"/>                |    
|---------------------------------|:---------------------------:|  
| Yuying Wang        |    Yang Zheng|   


## Repository <a name="description-of-files"><a/> 

| File                            |      Description            |   
|---------------------------------|:---------------------------:|   
| `data_exploration.ipynb`          |     exploratory data analysis on the yelp dataset                      |   
| `frequent_itemset_mining.ipynb`   |   mining frequent itemsets using SON, A-Priori algorithm |      
| `similar_businesses.py`   |  detecting similar business pairs using MinHash and LSH algorithm  |    
| `hybrid_recommender_system.py`   | combines different types of recommendation techniques including content-based filtering, model-based collaborative filtering, user-based CF, and item-based CF   |      
 

## Table of Contents
* [Data Exploration](#data-exploration)
* [Frequent Itemset Mining](#frequent-itemset-mining)
* [Similar Businesses](#similar-businesses)
* [Hybrid Recommender System](#hybrid-recommender-system)


## Data Exploration <a name="data-exploration"/>

### Data Description
Full Description: https://www.yelp.com/dataset/documentation/main 

Some Interesting Findings:
- TBA
- 


## Frequent Itemset Mining <a name="frequent-itemset-mining"/>
 Not surprisingly, we found that the resturants are **geographically close** to each other or they **serve similar food** (maybe have similar business names) in almost all frequent sets. (e.g. Ramen Sora, Sushi House Goyemon, Monta Ramen)


## Similar Businesses <a name="similar-businesses"/>
First we use MinHash to generate signature of each business, then apply LSH to find all candidate pairs, and finally do a full pass to eliminate all false positives. We spent quite some time on designing hash functions, and suprisingly, we achieve **precision=1.0 and recall=1.0** .


## Hybrid Recommender System <a name="hybrid-recommender-system"/>
The ratings range from 1 to 5, the error distribution on testing data: 
<img alt="Error distribution on testing data" src="imgs/rec-error-dist.png" width="420"/>  
about 98% prediction error are less than 1.0, and the overall RMSE is 0.9782, which is much better than any individual recommender system.
 

## Dependencies <a name="dependencies"/>
* Spark 2.4
* Python 3.6
* JDK 1.8
