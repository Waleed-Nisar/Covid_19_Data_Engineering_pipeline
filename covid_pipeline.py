#!/usr/bin/env python3
"""
COVID-19 Data Engineering Pipeline
A complete ETL pipeline that extracts, transforms, and loads COVID-19 data
"""

import pandas as pd
import requests
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import logging
import os
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CovidDataPipeline:
    def __init__(self):
        self.base_url = "https://disease.sh/v3/covid-19"
        self.db_path = "covid_data.db"
        self.output_dir = Path("output")
        self.output_dir.mkdir(exist_ok=True)
        
    def extract_data(self):
        """Extract data from COVID-19 API"""
        logger.info("Starting data extraction...")
        
        # Get global statistics
        global_url = f"{self.base_url}/all"
        global_response = requests.get(global_url)
        global_data = global_response.json()
        
        # Get country data
        countries_url = f"{self.base_url}/countries"
        countries_response = requests.get(countries_url)
        countries_data = countries_response.json()
        
        # Get historical data (last 30 days)
        historical_url = f"{self.base_url}/historical/all?lastdays=30"
        historical_response = requests.get(historical_url)
        historical_data = historical_response.json()
        
        logger.info("Data extraction completed successfully")
        return global_data, countries_data, historical_data
    
    def transform_data(self, global_data, countries_data, historical_data):
        """Transform and clean the extracted data"""
        logger.info("Starting data transformation...")
        
        # Transform global data - flatten nested objects
        global_df = pd.json_normalize(global_data)
        global_df['extraction_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Transform countries data - flatten nested objects
        countries_df = pd.json_normalize(countries_data)
        countries_df['extraction_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Clean countries data - handle missing values and convert to numeric
        numeric_cols = ['cases', 'deaths', 'recovered', 'active', 'critical', 'population']
        for col in numeric_cols:
            if col in countries_df.columns:
                countries_df[col] = pd.to_numeric(countries_df[col], errors='coerce').fillna(0)
        
        # Calculate rates safely
        countries_df['mortality_rate'] = ((countries_df['deaths'] / countries_df['cases'].replace(0, 1)) * 100).round(2)
        countries_df['recovery_rate'] = ((countries_df['recovered'] / countries_df['cases'].replace(0, 1)) * 100).round(2)
        
        # Transform historical data
        historical_df = pd.DataFrame({
            'date': list(historical_data['cases'].keys()),
            'cases': list(historical_data['cases'].values()),
            'deaths': list(historical_data['deaths'].values()),
            'recovered': list(historical_data['recovered'].values())
        })
        
        historical_df['date'] = pd.to_datetime(historical_df['date']).dt.strftime('%Y-%m-%d')
        historical_df['daily_cases'] = historical_df['cases'].diff().fillna(0)
        historical_df['daily_deaths'] = historical_df['deaths'].diff().fillna(0)
        
        # Ensure all numeric columns are properly converted
        numeric_cols = ['cases', 'deaths', 'recovered', 'daily_cases', 'daily_deaths']
        for col in numeric_cols:
            historical_df[col] = pd.to_numeric(historical_df[col], errors='coerce').fillna(0)
        
        logger.info("Data transformation completed successfully")
        return global_df, countries_df, historical_df
    
    def load_data(self, global_df, countries_df, historical_df):
        """Load data into SQLite database"""
        logger.info("Starting data loading...")
        
        with sqlite3.connect(self.db_path) as conn:
            # Load data into tables
            global_df.to_sql('global_stats', conn, if_exists='replace', index=False)
            countries_df.to_sql('country_stats', conn, if_exists='replace', index=False)
            historical_df.to_sql('historical_data', conn, if_exists='replace', index=False)
            
            # Create indexes for better performance
            conn.execute("CREATE INDEX IF NOT EXISTS idx_country ON country_stats(country)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_date ON historical_data(date)")
            
        logger.info("Data loading completed successfully")
    
    def create_visualizations(self):
        """Create data visualizations"""
        logger.info("Creating visualizations...")
        
        with sqlite3.connect(self.db_path) as conn:
            # Read data from database
            countries_df = pd.read_sql_query("SELECT * FROM country_stats", conn)
            historical_df = pd.read_sql_query("SELECT * FROM historical_data", conn)
            
        # Set style
        plt.style.use('seaborn-v0_8')
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('COVID-19 Data Analysis Dashboard', fontsize=16, fontweight='bold')
        
        # 1. Top 10 countries by cases
        top_countries = countries_df.nlargest(10, 'cases')
        axes[0, 0].bar(range(len(top_countries)), top_countries['cases'])
        axes[0, 0].set_title('Top 10 Countries by Total Cases')
        axes[0, 0].set_xlabel('Countries')
        axes[0, 0].set_ylabel('Total Cases')
        axes[0, 0].set_xticks(range(len(top_countries)))
        axes[0, 0].set_xticklabels(top_countries['country'], rotation=45, ha='right')
        
        # 2. Historical trend
        historical_df['date'] = pd.to_datetime(historical_df['date'])
        axes[0, 1].plot(historical_df['date'], historical_df['daily_cases'], label='Daily Cases', alpha=0.7)
        axes[0, 1].plot(historical_df['date'], historical_df['daily_deaths'], label='Daily Deaths', alpha=0.7)
        axes[0, 1].set_title('Daily Cases and Deaths Trend (Last 30 Days)')
        axes[0, 1].set_xlabel('Date')
        axes[0, 1].set_ylabel('Count')
        axes[0, 1].legend()
        axes[0, 1].tick_params(axis='x', rotation=45)
        
        # 3. Mortality rate distribution
        mortality_data = countries_df[countries_df['mortality_rate'] > 0]['mortality_rate']
        axes[1, 0].hist(mortality_data, bins=20, alpha=0.7, edgecolor='black')
        axes[1, 0].set_title('Distribution of Mortality Rates by Country')
        axes[1, 0].set_xlabel('Mortality Rate (%)')
        axes[1, 0].set_ylabel('Number of Countries')
        
        # 4. Cases vs Deaths scatter plot
        sample_countries = countries_df.sample(min(50, len(countries_df)))
        scatter = axes[1, 1].scatter(sample_countries['cases'], sample_countries['deaths'], 
                                   alpha=0.6, s=50)
        axes[1, 1].set_title('Cases vs Deaths (Sample of Countries)')
        axes[1, 1].set_xlabel('Total Cases')
        axes[1, 1].set_ylabel('Total Deaths')
        axes[1, 1].set_xscale('log')
        axes[1, 1].set_yscale('log')
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'covid_dashboard.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info("Visualizations created successfully")
    
    def generate_report(self):
        """Generate a summary report"""
        logger.info("Generating summary report...")
        
        with sqlite3.connect(self.db_path) as conn:
            global_stats = pd.read_sql_query("SELECT * FROM global_stats", conn)
            countries_stats = pd.read_sql_query("SELECT * FROM country_stats", conn)
            
        report = f"""
COVID-19 Data Engineering Pipeline Report
========================================
Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

GLOBAL STATISTICS
-----------------
Total Cases: {global_stats['cases'].iloc[0]:,}
Total Deaths: {global_stats['deaths'].iloc[0]:,}
Total Recovered: {global_stats['recovered'].iloc[0]:,}
Active Cases: {global_stats['active'].iloc[0]:,}
Global Mortality Rate: {(global_stats['deaths'].iloc[0] / global_stats['cases'].iloc[0] * 100):.2f}%

TOP 5 MOST AFFECTED COUNTRIES
-----------------------------
"""
        
        top_5 = countries_stats.nlargest(5, 'cases')[['country', 'cases', 'deaths', 'mortality_rate']]
        for _, row in top_5.iterrows():
            report += f"{row['country']}: {row['cases']:,} cases, {row['deaths']:,} deaths ({row['mortality_rate']:.2f}% mortality)\n"
        
        report += f"""
PIPELINE STATISTICS
------------------
Countries Processed: {len(countries_stats)}
Database Size: {os.path.getsize(self.db_path) / 1024:.2f} KB
Processing Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Files Generated:
- covid_data.db (SQLite database)
- covid_dashboard.png (Visualization dashboard)
- covid_report.txt (This report)
"""
        
        with open(self.output_dir / 'covid_report.txt', 'w') as f:
            f.write(report)
        
        logger.info("Report generated successfully")
        return report
    
    def run_pipeline(self):
        """Execute the complete ETL pipeline"""
        try:
            logger.info("Starting COVID-19 Data Pipeline...")
            
            # Extract
            global_data, countries_data, historical_data = self.extract_data()
            
            # Transform
            global_df, countries_df, historical_df = self.transform_data(
                global_data, countries_data, historical_data
            )
            
            # Load
            self.load_data(global_df, countries_df, historical_df)
            
            # Analyze and Visualize
            self.create_visualizations()
            
            # Report
            report = self.generate_report()
            
            logger.info("Pipeline completed successfully!")
            print("\n" + "="*50)
            print("COVID-19 DATA PIPELINE COMPLETED")
            print("="*50)
            print(f"✓ Data extracted from {len(countries_data)} countries")
            print(f"✓ Database created: {self.db_path}")
            print(f"✓ Visualizations saved to: {self.output_dir}")
            print(f"✓ Report generated: {self.output_dir}/covid_report.txt")
            print("="*50)
            
            return True
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            return False

if __name__ == "__main__":
    pipeline = CovidDataPipeline()
    pipeline.run_pipeline()
