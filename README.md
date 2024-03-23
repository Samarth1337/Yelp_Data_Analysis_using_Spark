# Yelp_analysis-Exploring_User_Recommendations_and_Business_Insights

## Project Overview:
For this project, I undertook a comprehensive exploration of the Yelp dataset to gain insights into user behavior and preferences regarding restaurants. Leveraging my expertise in data mining and analytics, I focused on understanding the factors influencing participants' likelihood to recommend their favorite restaurants to others. The project involved employing experimental design methodologies, including between-subject and within-subject testing, to investigate the effects of offering discounts and opening new locations on restaurant suggestions. By conducting statistical analyses, including 2-way ANOVA tests, I uncovered significant main effects of time but not of scenarios, highlighting the nuanced dynamics at play in user recommendations.

## Solution Highlights:
To address the research objectives, I designed and implemented Python scripts using Spark RDD to perform data processing and analysis. 

### In Task 1, 
I developed a script (task1.py) to automatically extract key insights from the Yelp review dataset, such as the total number of reviews, the number of reviews in 2018, the number of distinct users who wrote reviews, and the top users and businesses based on review count.

### In Task 2, 
I focused on optimizing data processing performance by partitioning the dataset and comparing default Spark partitioning with a customized partitioning approach. By implementing a custom partitioning function in task2.py, I demonstrated improvements in execution time for specific tasks, showcasing the importance of performance optimization in large-scale data processing pipelines.

### In Task 3, 
I expanded my analysis to explore relationships between multiple datasets, including review information and business information. Using Spark RDD, I developed a script (task3.py) to calculate the average star ratings for each city and compared execution times between two methods for identifying the top cities with the highest average star ratings.

## Technologies Used:

Python: Leveraged Python programming language for script development, ensuring compatibility with Spark RDD and adherence to assignment requirements.
Spark RDD: Utilized Spark Resilient Distributed Datasets (RDD) for distributed data processing, enabling efficient handling of large-scale datasets and parallel execution of data transformation operations.
JSON: Employed JSON (JavaScript Object Notation) format for data input and output, facilitating interoperability and ease of integration with Spark RDD operations.
Apache Spark: Leveraged Apache Spark framework for distributed data processing, enabling seamless scalability and fault tolerance for processing large volumes of data.

## Data Description:
The Yelp dataset serves as the foundation for this project, offering a rich source of user-generated reviews and business information. Comprising subsets of the original Yelp Dataset, the data includes two primary files: business.json and test_review.json. These files contain a diverse array of attributes, including business IDs, user IDs, review text, star ratings, and geographical information such as city and state. The dataset presents a real-world scenario for analysis, reflecting the dynamic interactions between users and businesses across various locations. By working with this dataset, I gained exposure to the complexities of real-world data and developed strategies to extract valuable insights for business intelligence and decision-making purposes.
