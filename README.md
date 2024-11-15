# Phase 1: Email Processing and Market Data Collection Pipeline

---

## Overview

This project (Phase 1) is designed to develop a comprehensive email processing and market data collection pipeline. The core functionality focuses on extracting emails from Gmail, processing and cleaning the content, handling incremental updates, and synchronizing with MongoDB to create a centralized and accessible data store. Over time, the project will expand to incorporate external news feeds, implement advanced topic modeling, and ultimately support an extensive data warehouse of market and economic data.

### Key Functionalities

- **Background Email Processing**: Continuous email processing with sleep prevention, automated backups, and progress notifications.
- **Email Extraction and Filtering**: Extracts and filters emails based on specified criteria, handling large volumes of data while respecting Gmail API rate limits.
- **Incremental Data Handling**: Manages incremental email updates to avoid duplicate entries in the dataset.
- **Data Synchronization with MongoDB**: Synchronizes email data with a MongoDB collection, ensuring consistency between the JSON dataset and the database. Supports both Atlas Clusters and Community Edition. 
    - **Local MongoDB Instance Provisioning**: Confirmed functionality to provision and connect to a local MongoDB instance via the Community Edition, with automatic syncing and database updates.
- **Progress Tracking and Notifications**: Real-time progress tracking with email notifications and automated backups every 1000 documents.
- **Sleep Prevention**: Keeps system active during long-running processes using native macOS capabilities.
- **Data Quality Analysis**: Verifies data quality in MongoDB, examining factors such as content length, top senders, daily distribution, and keyword analysis.
- **Logging and Error Handling**: Implements robust logging to facilitate debugging and ensure traceability in case of errors or failed processes.

### Roadmap for Future Enhancements

- **Topic Modeling and News Feed Integration**: Expand data collection by integrating with open-source news feeds and implementing topic modeling for in-depth content analysis.
- **Future Support for DuckDb**: Plan to integrate DuckDB for fast analytical workloads, especially useful for feature stores and large dataframes, enabling more efficient handling of large datasets in future analytical expansions.
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
   git clone https://github.com/gabrielanyosa1/ETLNewsletters
   cd ~/ETLNewsletters
   ```

2. **Environment Configuration**:
   - Make sure to set up your Gmail API credentials (`credentials.json`) and place it in the project root.
   - Create and configure your `.env` file:
     ```env
     MONGODB_URI=your_mongodb_uri
     GMAIL_USER=your_gmail_address
     GMAIL_APP_PASSWORD=your_app_specific_password
     NOTIFICATION_EMAIL=your_notification_email
     ```
      - Ensure MongoDB is accessible by configuring your `.env` file or setting the `MONGODB_URI` environment variable.
   - Configure MongoDB connection (supports local or cloud)
   - Create a (`filter_senders.json`) with the desired sender email addresses to fetch emails from.

3. **System Requirements**:
   - Python 3.12+
   - MongoDB (local or cloud)
   - MacOS (for native sleep prevention)

> **Note**: Detailed requirements and dependencies will be included in `requirements.txt` in future updates as the project evolves.

---

## Usage

1. **Test System Components**:
   ```bash
   python system_test.py
   ```
   Verifies:
   - Environment variables
   - MongoDB connection
   - Email notifications
   - Sleep prevention
   - Backup system

2. **Start Background Processing**:
   ```bash
   python background_processor.py
   ```
   Features:
   - Progress tracking with tqdm
   - Email notifications every 1000 documents
   - Automatic backups every 30 minutes
   - Sleep prevention
   - Checkpointing for resume capability

3. **Verify Data Consistency**:
   ```bash
   python verify_state.py
   ```

4. **Sync Data with MongoDB**:
   ```bash
   python sync_mongodb.py
   ```

5. **Verify MongoDB Data Quality**:
   ```bash
   python verify_mongo_data.py
   ```


---

## Project Structure

```
├── background_processor.py      # Background processing with progress tracking
├── system_test.py              # System component testing
├── gmail_extract.py            # Main email extraction and processing
├── incremental_email_handler.py # Incremental update handling
├── sync_mongodb.py             # MongoDB synchronization
├── verify_state.py             # Data consistency verification
├── verify_mongo_data.py        # MongoDB data quality analysis
├── mongo_loader.py             # MongoDB connection management
├── run_analysis.py             # Grafana dashboard data analysis
├── verify_grafana_setup.py     # Grafana integration verification
├── verify_grafana_connection.py # Grafana connection testing
├── verify_grafana_data_format.py # Data format validation for Grafana
├── analysis/                   # Analysis modules
│   ├── __init__.py
│   ├── config/                 # Configuration management
│   │   └── grafana_config.py   # Grafana configuration
│   └── eda/                    # Exploratory Data Analysis
│       ├── __init__.py
│       ├── subject_analyzer.py # Email subject analysis
│       └── grafana_publisher.py # Grafana dashboard management
├── .env                        # Environment configuration
├── credentials.json            # Gmail API credentials
├── README.md                   # Project documentation
└── backups/                    # Automated backup storage
```

### Key Files and Modules

- **`background_processor.py`**: Manages continuous email processing with progress tracking, notifications, and automated backups. Features sleep prevention for long-running processes and checkpoint management.

- **`system_test.py`**: Tests core system components including MongoDB connection, email notifications, sleep prevention, and backup functionality.

- **`gmail_extract.py`**: Main email extraction script connecting to Gmail API, with rate limiting and error handling.

- **`incremental_email_handler.py`**: Handles incremental updates, avoiding duplicates and managing backups.

- **`sync_mongodb.py`**: Synchronizes MongoDB collection with JSON dataset, minimizing redundant storage.

- **`verify_state.py`**: Verifies data consistency between JSON and MongoDB.

- **`verify_mongo_data.py`**: Analyzes MongoDB data quality and provides statistics.

- **`mongo_loader.py`**: Manages MongoDB connections, data loading, and indexing.

- **`run_analysis.py`**: Manages data analysis and Grafana dashboard creation for email insights.

- **`analysis/eda/subject_analyzer.py`**: Analyzes email subjects for trends and categories.

- **`analysis/eda/grafana_publisher.py`**: Handles Grafana dashboard creation and updates.

- **`analysis/config/grafana_config.py`**: Manages Grafana Cloud configuration.


---

## Functionality Overview

### 1. **Background Processing System**

   - **Continuous Operation**: Runs continuously with sleep prevention
   - **Progress Tracking**: Real-time progress with tqdm
   - **Notifications**: Email updates every 1000 documents
   - **Automated Backups**: Regular backups every 30 minutes
   - **Checkpointing**: Resume capability after interruptions

### 2. **Email Extraction and Processing**

   - **Gmail API Integration**: Fetches emails with rate limiting
   - **Content Processing**: Decodes and cleans email content
   - **Filtering**: Applies criteria to select relevant emails

### 3. **Data Management**

   - **Incremental Updates**: Avoids duplicates in dataset
   - **MongoDB Sync**: Maintains consistency across storage
   - **Backup System**: Regular automated backups
   - **Data Quality**: Continuous verification and analysis

### 4. **System Monitoring**

   - **Progress Tracking**: Visual progress indicators
   - **Email Notifications**: Regular status updates
   - **Error Handling**: Comprehensive error capture
   - **Logging**: Detailed activity logging

### 5. **Data Visualization with Grafana**

   - **Real-time Dashboard**: Visualizes email analysis in real-time
   - **Category Analysis**: Shows distribution of email categories
   - **Theme Tracking**: Displays most common newsletter themes
   - **Time Series Analysis**: Tracks category trends over time

---

## Configuration

### Background Processor Settings

```python
class BackgroundProcessor:
    NOTIFICATION_THRESHOLD = 1000  # emails
    BACKUP_INTERVAL = 1800        # seconds (30 minutes)
```

### MongoDB Configuration

- Database: gmail_archive
- Collection: emails
- Indexes:
  - id (unique)
  - parsedDate (sorted)
  - from
  - subject

### Email Processing

- Rate Limiting: 2 calls per second
- Batch Size: 50 emails
- Automatic retries with exponential backoff

### Grafana Configuration

```python
# In .env file
GRAFANA_INSTANCE_URL=your_grafana_cloud_url
GRAFANA_SA_TOKEN=your_service_account_token
GRAFANA_ORG_ID=your_org_id  # Optional
GRAFANA_DASHBOARD_FOLDER=Email Analysis  # Optional
```

Required environment variables:
- GRAFANA_INSTANCE_URL
- GRAFANA_SA_TOKEN

### Data Visualization

The Grafana dashboard includes:
1. Time Series Graph: Newsletter Categories Over Time
2. Pie Chart: Newsletter Category Distribution
3. Bar Gauge: Most Common Newsletter Themes

Known Issues:
- Dashboard data may change on refresh
- Category distribution shows duplicate entries
- Theme panel requires aggregation improvements

---

## Development Notes

### Testing

- Run system tests before processing:
  ```bash
  python system_test.py
  ```
- Monitor logs in `background_processor.log`
- Check MongoDB stats regularly

### Best Practices

- Keep Gmail API credentials secure
- Regular backup verification
- Monitor system resources
- Check logs for errors

### Grafana Integration

- Test Grafana setup:
  ```bash
  python verify_grafana_setup.py
  ```
- Verify data format:
  ```bash
  python verify_grafana_data_format.py
  ```
- Run analysis and create dashboard:
  ```bash
  python run_analysis.py
  ```

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

6. **Enhanced Data Governance and Security**
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