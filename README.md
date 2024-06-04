Serverless architecture to streamline data integration from Spotify to Snowflake! 

In this workflow:

1. **Spotify Data Integration**: We extract data from Spotify's API.
2. **AWS Lambda Functions**: These functions handle the extraction, transformation, and loading (ETL) processes.
3. **Amazon S3**: Serves as a temporary storage for the data.
4. **Data Processing**: Additional Lambda functions process the data stored in S3.
5. **Snowflake Data Warehouse**: The processed data is loaded into Snowflake for analysis and reporting.

This architecture ensures scalability, cost-efficiency, and real-time data processing capabilities, making it an ideal solution for handling large volumes of data. 🎧💡

Architectural Diagram
![diagram-export-6-3-2024-7_44_18-AM](https://github.com/devs44/USA_HIT_SONGS_ETL_PIPELINE/assets/62928989/8574fefb-b250-488d-ac68-a55003231aff)

BI Dashboard

![Untitled_page-0001](https://github.com/devs44/USA_HIT_SONGS_ETL_PIPELINE/assets/62928989/7481a5dd-945d-4ec9-b6ca-c3f742ec9988)
