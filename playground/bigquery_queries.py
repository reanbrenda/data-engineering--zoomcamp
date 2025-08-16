#!/usr/bin/env python3
"""
BigQuery Queries and Visualizations for Network Rail Movement Data
Author: Brenda
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from google.cloud import bigquery
from google.oauth2 import service_account
import json
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Set up plotting style
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

class NetworkRailAnalytics:
    def __init__(self, credentials_path='raildata-469117-c69f6f420e79.json'):
        """Initialize BigQuery client with credentials."""
        try:
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            self.client = bigquery.Client(credentials=credentials, project=credentials.project_id)
            print(f"✅ Connected to BigQuery project: {credentials.project_id}")
        except Exception as e:
            print(f"❌ Error connecting to BigQuery: {e}")
            self.client = None
    
    def run_query(self, query):
        """Run a BigQuery SQL query and return results as DataFrame."""
        if not self.client:
            print("❌ BigQuery client not initialized")
            return None
        
        try:
            print(f"🔍 Running query...")
            df = self.client.query(query).to_dataframe()
            print(f"✅ Query completed! Returned {len(df)} rows")
            return df
        except Exception as e:
            print(f"❌ Query failed: {e}")
            return None
    
    def get_basic_stats(self):
        """Get basic statistics about the movements data."""
        query = """
        SELECT 
            COUNT(*) as total_movements,
            COUNT(DISTINCT train_id) as unique_trains,
            COUNT(DISTINCT toc_id) as unique_tocs,
            COUNT(DISTINCT DATE(actual_timestamp)) as unique_dates,
            MIN(actual_timestamp) as earliest_movement,
            MAX(actual_timestamp) as latest_movement
        FROM `raildata-469117.networkrail.movements`
        """
        return self.run_query(query)
    
    def get_movements_by_toc(self):
        """Get movement counts by Train Operating Company."""
        query = """
        SELECT 
            toc_id,
            COUNT(*) as movement_count,
            COUNT(CASE WHEN event_type = 'DEPARTURE' THEN 1 END) as departures,
            COUNT(CASE WHEN event_type = 'ARRIVAL' THEN 1 END) as arrivals
        FROM `raildata-469117.networkrail.movements`
        GROUP BY toc_id
        ORDER BY movement_count DESC
        LIMIT 20
        """
        return self.run_query(query)
    
    def get_punctuality_analysis(self):
        """Analyze train punctuality."""
        query = """
        SELECT 
            variation_status,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
        FROM `raildata-469117.networkrail.movements`
        GROUP BY variation_status
        ORDER BY count DESC
        """
        return self.run_query(query)
    
    def get_hourly_patterns(self):
        """Get movement patterns by hour of day."""
        query = """
        SELECT 
            EXTRACT(HOUR FROM actual_timestamp) as hour_of_day,
            COUNT(*) as movement_count,
            COUNT(CASE WHEN event_type = 'DEPARTURE' THEN 1 END) as departures,
            COUNT(CASE WHEN event_type = 'ARRIVAL' THEN 1 END) as arrivals
        FROM `raildata-469117.networkrail.movements`
        GROUP BY hour_of_day
        ORDER BY hour_of_day
        """
        return self.run_query(query)
    
    def get_recent_movements(self, hours=24):
        """Get recent movements from the last N hours."""
        query = f"""
        SELECT 
            train_id,
            event_type,
            toc_id,
            variation_status,
            actual_timestamp,
            loc_stanox,
            planned_timestamp,
            ROUND((actual_timestamp - planned_timestamp) / 60, 2) as delay_minutes
        FROM `raildata-469117.networkrail.movements`
        WHERE actual_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
        ORDER BY actual_timestamp DESC
        LIMIT 50
        """
        return self.run_query(query)
    
    def create_visualizations(self):
        """Create comprehensive visualizations of the data."""
        print("🎨 Creating visualizations...")
        
        # Create figure with subplots
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Network Rail Movement Analytics', fontsize=16, fontweight='bold')
        
        # 1. Movements by TOC
        print("📊 Creating TOC analysis...")
        toc_data = self.get_movements_by_toc()
        if toc_data is not None and not toc_data.empty:
            top_10_toc = toc_data.head(10)
            axes[0, 0].barh(top_10_toc['toc_id'], top_10_toc['movement_count'])
            axes[0, 0].set_title('Top 10 TOCs by Movement Count')
            axes[0, 0].set_xlabel('Movement Count')
            axes[0, 0].set_ylabel('TOC ID')
        
        # 2. Punctuality Analysis
        print("⏰ Creating punctuality analysis...")
        punctuality_data = self.get_punctuality_analysis()
        if punctuality_data is not None and not punctuality_data.empty:
            axes[0, 1].pie(punctuality_data['count'], labels=punctuality_data['variation_status'], 
                           autopct='%1.1f%%', startangle=90)
            axes[0, 1].set_title('Train Punctuality Distribution')
        
        # 3. Hourly Patterns
        print("🕐 Creating hourly patterns...")
        hourly_data = self.get_hourly_patterns()
        if hourly_data is not None and not hourly_data.empty:
            axes[1, 0].plot(hourly_data['hour_of_day'], hourly_data['movement_count'], 
                           marker='o', linewidth=2, markersize=8)
            axes[1, 0].set_title('Movement Patterns by Hour of Day')
            axes[1, 0].set_xlabel('Hour of Day (24h)')
            axes[1, 0].set_ylabel('Movement Count')
            axes[1, 0].grid(True, alpha=0.3)
        
        # 4. Event Type Distribution
        print("🚂 Creating event type analysis...")
        event_query = """
        SELECT 
            event_type,
            COUNT(*) as count
        FROM `raildata-469117.networkrail.movements`
        GROUP BY event_type
        """
        event_data = self.run_query(event_query)
        if event_data is not None and not event_data.empty:
            axes[1, 1].bar(event_data['event_type'], event_data['count'], 
                           color=['#FF6B6B', '#4ECDC4', '#45B7D1'])
            axes[1, 1].set_title('Movement Types Distribution')
            axes[1, 1].set_ylabel('Count')
        
        plt.tight_layout()
        plt.savefig('networkrail_analytics.png', dpi=300, bbox_inches='tight')
        print("💾 Visualization saved as 'networkrail_analytics.png'")
        plt.show()
    
    def interactive_queries(self):
        """Interactive query interface."""
        print("\n🔍 Interactive Query Interface")
        print("=" * 50)
        
        while True:
            print("\nChoose an option:")
            print("1. Basic Statistics")
            print("2. Movements by TOC")
            print("3. Punctuality Analysis")
            print("4. Hourly Patterns")
            print("5. Recent Movements (Last 24h)")
            print("6. Custom SQL Query")
            print("7. Create All Visualizations")
            print("0. Exit")
            
            choice = input("\nEnter your choice (0-7): ").strip()
            
            if choice == '0':
                print("👋 Goodbye!")
                break
            elif choice == '1':
                result = self.get_basic_stats()
                if result is not None:
                    print("\n📊 Basic Statistics:")
                    print(result.to_string(index=False))
            elif choice == '2':
                result = self.get_movements_by_toc()
                if result is not None:
                    print("\n🚂 Movements by TOC:")
                    print(result.to_string(index=False))
            elif choice == '3':
                result = self.get_punctuality_analysis()
                if result is not None:
                    print("\n⏰ Punctuality Analysis:")
                    print(result.to_string(index=False))
            elif choice == '4':
                result = self.get_hourly_patterns()
                if result is not None:
                    print("\n🕐 Hourly Patterns:")
                    print(result.to_string(index=False))
            elif choice == '5':
                result = self.get_recent_movements()
                if result is not None:
                    print("\n🕐 Recent Movements (Last 24h):")
                    print(result.to_string(index=False))
            elif choice == '6':
                custom_query = input("\nEnter your SQL query: ")
                if custom_query.strip():
                    result = self.run_query(custom_query)
                    if result is not None:
                        print("\n📋 Query Results:")
                        print(result.to_string(index=False))
            elif choice == '7':
                self.create_visualizations()
            else:
                print("❌ Invalid choice. Please try again.")

def main():
    """Main function to run the analytics."""
    print("🚂 Network Rail Movement Analytics")
    print("=" * 40)
    
    # Initialize analytics
    analytics = NetworkRailAnalytics()
    
    if analytics.client:
        # Run interactive queries
        analytics.interactive_queries()
    else:
        print("❌ Failed to initialize BigQuery client. Please check your credentials.")

if __name__ == "__main__":
    main()
