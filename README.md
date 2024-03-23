# Yelp_analysis-Exploring_User_Recommendations_and_Business_Insights

## Project Overview: Yelp Data Analysis

In this project, I ran a comprehensive exploration of Yelp's vast dataset to uncover valuable insights into user recommendations and business dynamics. Leveraging Spark, a powerful distributed computing framework, I dove into three key tasks aimed at unraveling the intricacies of user behavior and business performance within the Yelp ecosystem.

## Solution Highlights:
To address the research objectives, I designed and implemented Python scripts using Spark RDD to perform data processing and analysis. 

### Task 1: Data Exploration
The first task entailed a meticulous examination of review data extracted from Yelp's repository. Through Python scripts powered by Spark RDDs, I tackled various inquiries, including the total number of reviews, reviews in the year 2018, distinct users contributing reviews, top users based on review count, distinct businesses reviewed, and top businesses by review count. By harnessing Spark's parallel processing capabilities, I efficiently extracted and analyzed key metrics, shedding light on user engagement and business popularity.

### Task 2: Partition Optimization
With an eye toward performance optimization, the second task focused on partitioning strategies within Spark RDDs. By customizing partition functions, I aimed to enhance the efficiency of map and reduce tasks while processing Yelp's review data. Through comparative analysis, I evaluated the impact of default versus customized partitioning on execution time, partition count, and item distribution. This optimization endeavor underscored the importance of strategic data partitioning in maximizing processing efficiency within Spark.

### Task 3: Exploration on Multiple Datasets
The final task delved into a comprehensive analysis encompassing both review and business datasets. By integrating disparate data sources, I sought to derive nuanced insights into user preferences across different cities. Leveraging Spark's capabilities, I computed average star ratings for each city, facilitating a deeper understanding of regional variations in user sentiment. Additionally, I conducted a comparative analysis of execution times between two methodologies for identifying top cities by average star ratings, shedding light on optimal approaches for aggregating and processing multi-dimensional data.

## Technologies Used:

Python: Leveraged Python programming language for script development, ensuring compatibility with Spark RDD and adherence to assignment requirements.
Spark RDD: Utilized Spark Resilient Distributed Datasets (RDD) for distributed data processing, enabling efficient handling of large-scale datasets and parallel execution of data transformation operations.
JSON: Employed JSON (JavaScript Object Notation) format for data input and output, facilitating interoperability and ease of integration with Spark RDD operations.
Apache Spark: Leveraged Apache Spark framework for distributed data processing, enabling seamless scalability and fault tolerance for processing large volumes of data.

## Data Description:
The Yelp dataset serves as the foundation for this project, offering a rich source of user-generated reviews and business information. Comprising subsets of the original Yelp Dataset, the data includes two primary files: business.json and test_review.json. These files contain a diverse array of attributes, including business IDs, user IDs, review text, star ratings, and geographical information such as city and state. The dataset presents a real-world scenario for analysis, reflecting the dynamic interactions between users and businesses across various locations. By working with this dataset, I gained exposure to the complexities of real-world data and developed strategies to extract valuable insights for business intelligence and decision-making purposes.
