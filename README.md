# Phase 1: Email Processing and Market Data Collection Pipeline

---

## Overview

This project (Phase 1) is designed to develop a comprehensive email processing and market data collection pipeline. The core functionality focuses on extracting emails from Gmail, processing and cleaning the content, handling incremental updates, and synchronizing with MongoDB to create a centralized and accessible data store. Over time, the project will expand to incorporate external news feeds, implement advanced topic modeling, and ultimately support an extensive data warehouse of market and economic data.

### Key Functionalities

- **Email Extraction and Filtering**: Extracts and filters emails based on specified criteria, handling large volumes of data while respecting Gmail API rate limits.
- **Incremental Data Handling**: Manages incremental email updates to avoid duplicate entries in the dataset.
- **Data Synchronization with MongoDB**: Synchronizes email data with a MongoDB collection, ensuring consistency between the JSON dataset and the database.
- **Data Quality Analysis**: Verifies data quality in MongoDB, examining factors such as content length, top senders, daily distribution, and keyword analysis.
- **Logging and Error Handling**: Implements robust logging to facilitate debugging and ensure traceability in case of errors or failed processes.

### Roadmap for Future Enhancements

- **Topic Modeling and News Feed Integration**: Expand data collection by integrating with open-source news feeds and implementing topic modeling for in-depth content analysis.
- **Local MongoDB Instance Provisioning**: Add functionality to provision and connect to a local MongoDB instance as a backup or alternative to cloud storage if space constraints arise.
- **Data Warehouse Creation**: Develop a structured data warehouse to store, manage, and query large datasets, including financial market and economic data.
- **Advanced ML and Data Science Algorithms**: Implement advanced machine learning and data science models for market data analysis, including sentiment analysis, private markets insights, and equity forecasting.
- **Visualization and Quantitative Analysis**: Incorporate visualization tools and quant analysis techniques for insights into private companies, macroeconomic trends, and various financial assets.

---

## Table of Contents

1. [Installation](#installation)
2. [Usage](#usage)
3. [Project Structure](#project-structure)
4. [Functionality Overview](#functionality-overview)
5. [Roadmap](#roadmap)
6. [Contributing](#contributing)
7. [License](#license)

---

## Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. **Environment Configuration**:
   - Make sure to set up your Gmail API credentials (`credentials.json`) and place it in the project root.
   - Ensure MongoDB is accessible by configuring your `.env` file or setting the `MONGODB_URI` environment variable.
   - This project assumes a cloud MongoDB instance, though future updates will include options for a local instance.

> **Note**: Detailed requirements and dependencies will be included in `requirements.txt` in future updates as the project evolves.

---

## Usage

1. **Initialize Email Processing**:
   Run the main email extraction script to fetch and process emails from Gmail.
   ```bash
   python gmail_extract.py
   ```
   
2. **Verify Data Consistency**:
   Use the verification script to ensure consistency between JSON and MongoDB datasets.
   ```bash
   python verify_state.py
   ```
   
3. **Sync Data with MongoDB**:
   Synchronize the JSON dataset with MongoDB, ensuring only missing entries are added to the database.
   ```bash
   python sync_mongodb.py
   ```

4. **Verify MongoDB Data Quality**:
   Run the data quality verification script to analyze the MongoDB collection for completeness and accuracy.
   ```bash
   python verify_mongo_data.py
   ```

---

## Project Structure

```
├── gmail_extract.py             # Main script for email extraction, filtering, and processing.
├── incremental_email_handler.py # Handles incremental email updates to avoid duplicates.
├── sync_mongodb.py              # Synchronizes MongoDB collection with the JSON dataset.
├── verify_state.py              # Verifies consistency between JSON and MongoDB data.
├── verify_mongo_data.py         # Conducts data quality analysis on MongoDB data.
├── mongo_loader.py              # Manages MongoDB connection, data loading, and collection statistics.
├── .env                         # Environment variables, including MongoDB URI.
├── credentials.json             # Gmail API credentials (should be kept secure).
├── README.md                    # Comprehensive documentation of the project.
└── ...                          # Additional configuration and log files.
```

### Key Files and Modules

- **`gmail_extract.py`**: This is the main email extraction script. It connects to Gmail API, fetches emails, filters based on defined criteria, and processes email bodies. The script includes detailed logging, error handling, and rate limiting to manage Gmail API requests efficiently.
  
- **`incremental_email_handler.py`**: This module handles incremental updates, ensuring that new emails are merged with the existing dataset without creating duplicates. It also backs up the JSON file before updating and provides dataset statistics for easy monitoring.
  
- **`sync_mongodb.py`**: This script synchronizes the MongoDB collection with the JSON dataset, ensuring that only missing documents are added. This reduces redundant data storage in MongoDB and improves consistency.
  
- **`verify_state.py`**: This script verifies consistency between the JSON file and MongoDB, highlighting any discrepancies in document counts or email IDs.
  
- **`verify_mongo_data.py`**: This module performs a thorough analysis of the MongoDB data, including daily email distribution, top senders, content length, and subject keyword analysis. It helps to monitor data quality and identify issues such as missing fields or duplicate entries.
  
- **`mongo_loader.py`**: This module encapsulates MongoDB connection handling, data insertion, and index creation. It supports batch processing, error handling, and collection statistics retrieval. This will also be extended in future versions to support local MongoDB instance provisioning.

---

## Functionality Overview

### 1. **Email Extraction and Filtering**

   - **Process Overview**: The `gmail_extract.py` script connects to Gmail API, retrieves emails based on predefined criteria, decodes and cleans the email content, and stores the processed emails in JSON format.
   - **Rate Limiting and Error Handling**: Implements exponential backoff and rate limiting to respect Gmail API limits. Comprehensive error handling ensures that the process can continue despite intermittent API issues.

### 2. **Incremental Email Handling**

   - **Duplicate Avoidance**: The `incremental_email_handler.py` module tracks existing email IDs and merges new emails while avoiding duplicates.
   - **Data Backup**: Creates backups of the JSON file before each update to safeguard against data loss.
   - **Statistics**: Provides statistics on the total number of emails, date range, and top senders.

### 3. **MongoDB Synchronization**

   - **Data Sync**: The `sync_mongodb.py` script ensures MongoDB and JSON datasets are in sync. It identifies and inserts only missing emails to minimize storage redundancy.
   - **Logging and Monitoring**: Detailed logs provide insights into the number of documents inserted, skipped, or failed during the synchronization process.

### 4. **Data Quality Verification**

   - **Consistency Checks**: The `verify_state.py` script compares JSON and MongoDB data to identify discrepancies in email counts or IDs, highlighting potential issues with data integrity.
   - **Detailed Analysis**: The `verify_mongo_data.py` script conducts a thorough analysis of MongoDB data, including:
     - **Daily Distribution**: Tracks email counts by date.
     - **Top Senders**: Identifies the most frequent email senders.
     - **Content Statistics**: Analyzes content length and URL counts.
     - **Keyword Analysis**: Extracts and ranks keywords from email subjects.

### 5. **MongoDB Connection and Data Loading**

   - **Connection Management**: The `mongo_loader.py` module handles MongoDB connections, including error handling and automatic retries for reliable database access.
   - **Batch Processing**: Supports batch loading of email data, with duplicate handling and failure recovery to ensure smooth data ingestion.
   - **Indexing**: Automatically creates indexes for efficient querying and retrieval of large datasets.

---

## Roadmap

This project is under active development, with several key features planned for future releases:

1. **Integration with External News Feeds and Topic Modeling**
   - Implement connectors for open-source news feeds to gather relevant market and financial data.
   - Integrate topic modeling techniques (e.g., Latent Dirichlet Allocation) for clustering and categorizing news and email content.

2. **Provisioning for Local MongoDB Instance**
   - Add support for provisioning a local MongoDB instance on `localhost` to supplement or replace the cloud instance, depending on data volume requirements.

3. **Development of a Data Warehouse for Market Data**
   - Expand the data collection to form a comprehensive data warehouse of market data, including financial metrics, economic indicators, and relevant news content.

4. **Advanced Machine Learning and Data Science**
   - Incorporate advanced ML algorithms for:
     - **Market Sentiment Analysis**: Analyze sentiment trends across private and public markets.
     - **Equity Forecasting and Macro Analysis**: Forecast equity trends, perform macroeconomic analysis, and build quant models to assess private companies and financial assets.
   - Design robust forecasting models leveraging time-series analysis, multivariate regression, and deep learning for comprehensive financial insights.

5. **Data Visualization and Reporting**
   - Develop interactive dashboards and visualizations to provide insights into market trends, top-performing sectors, and sentiment shifts.
   - Use libraries like `matplotlib`, `Plotly`, or `D3.js` to create engaging and informative data visualizations.

6. **Enhanced

 Data Governance and Security**
   - Implement stricter data governance protocols to ensure the security and integrity of the email and market data.
   - Explore options for data encryption, user authentication, and audit logging to safeguard sensitive information.

---

## Contributing

Contributions to this project are welcome. If you have suggestions for improvements or want to contribute code, please follow these steps:

1. Fork the repository.
2. Create a new branch (`feature/your-feature-name`).
3. Commit your changes with a descriptive message.
4. Push to the branch.
5. Create a pull request, and the project maintainers will review your changes.

### Code Style and Guidelines

- Ensure all Python code follows [PEP 8](https://pep8.org/) guidelines.
- Document functions and modules with clear and concise docstrings.
- Provide comprehensive error handling and logging in all new code.
- Make sure tests cover edge cases and are included for any new functionality added.

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

## Contact

For any questions, suggestions, or issues, please contact the project maintainers via the repository’s issue tracker or pull request discussions.

---