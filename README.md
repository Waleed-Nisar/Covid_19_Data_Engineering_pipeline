# Covid_19 Data_Engineering Pipeline


A complete end-to-end data engineering project that demonstrates ETL (Extract, Transform, Load) processes using real-world COVID-19 data.

## 🚀 Features

- **Data Extraction**: Fetches real-time COVID-19 data from Disease.sh API
- **Data Transformation**: Cleans, processes, and enriches raw data
- **Data Loading**: Stores processed data in SQLite database with proper indexing
- **Data Analysis**: Generates insights and statistical summaries
- **Visualization**: Creates comprehensive dashboards and charts
- **Reporting**: Produces automated summary reports

## 📊 What This Pipeline Does

1. **Extracts** COVID-19 data from multiple API endpoints:
   - Global statistics
   - Country-wise data
   - Historical trends (30 days)

2. **Transforms** the data by:
   - Cleaning missing values
   - Calculating mortality and recovery rates
   - Computing daily changes
   - Adding timestamps

3. **Loads** data into a structured SQLite database with:
   - Proper table schemas
   - Performance indexes
   - Data validation

4. **Analyzes** and **Visualizes**:
   - Top affected countries
   - Daily trend analysis
   - Mortality rate distributions
   - Interactive dashboards

## 🛠️ Technical Stack

- **Python 3.8+**
- **pandas** - Data manipulation and analysis
- **requests** - HTTP API calls
- **SQLite** - Database storage
- **matplotlib/seaborn** - Data visualization
- **logging** - Pipeline monitoring

## 📁 Project Structure

```
covid-pipeline/
├── covid_pipeline.py      # Main pipeline script
├── requirements.txt       # Python dependencies
├── README.md             # Project documentation
├── output/               # Generated outputs
│   ├── covid_dashboard.png
│   └── covid_report.txt
└── covid_data.db         # SQLite database
```


## 🚦 Pipeline Monitoring

The pipeline includes comprehensive logging:
- Real-time progress updates
- Error handling and recovery
- Performance metrics
- Data quality checks
  
##  Visualization

![covid_dashboard](https://github.com/user-attachments/assets/808c0f19-bd38-4fdc-a179-c6df3866d847)


## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🔗 Data Source

Data provided by [Disease.sh](https://disease.sh/) - Open Disease Data API



