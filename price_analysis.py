import pandas as pd
import psycopg2
from psycopg2 import sql
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# PostgreSQL connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'database': 'retail',
    'user': os.getenv("postgres_db_user"),
    'password': os.getenv("postgres_db_password"),  # Change this to your actual password
    'port': 5432
}

def connect_to_postgres():
    """Establish a connection to PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Successfully connected to PostgreSQL")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise


def fetch_product_data(conn):
    """Fetch retail product information from the silver layer table."""
    query = """
    SELECT *
    FROM retail.public_silver_layer.retail_product_information
    """
    try:
        df = pd.read_sql(query, conn)
        logger.info(f"Fetched {len(df)} rows from retail_product_information table")
        return df
    except Exception as e:
        logger.error(f"Failed to fetch data: {e}")
        raise


def analyze_price_statistics(df):
    """Calculate basic price statistics."""
    logger.info("Analyzing price statistics...")
    
    if 'price_ron' in df.columns:
        stats = {
            'mean_price': df['price_ron'].mean(),
            'median_price': df['price_ron'].median(),
            'min_price': df['price_ron'].min(),
            'max_price': df['price_ron'].max(),
            'std_price': df['price'].std(),
            'total_products': len(df)
        }
        return stats
    else:
        logger.warning("'price' column not found in dataset")
        return None


def analyze_product_distribution(df):
    """Analyze product distribution by category or brand."""
    logger.info("Analyzing product distribution...")
    
    distribution = {}
    for col in df.columns:
        if col not in ['price_ron', 'id', 'created_at', 'updated_at']:
            if df[col].dtype == 'object':
                distribution[col] = df[col].value_counts()
    
    return distribution


def visualize_price_distribution(df, output_path='price_distribution.png'):
    """Create visualization of price distribution."""
    if 'price_ron' not in df.columns:
        logger.warning("Cannot visualize: 'price_ron' column not found")
        return
    
    logger.info("Creating price distribution visualization...")
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # Histogram
    axes[0].hist(df['price_ron'], bins=50, color='skyblue', edgecolor='black')
    axes[0].set_xlabel('Price')
    axes[0].set_ylabel('Frequency')
    axes[0].set_title('Price Distribution')
    axes[0].grid(True, alpha=0.3)
    
    # Box plot
    axes[1].boxplot(df['price_ron'])
    axes[1].set_ylabel('Price')
    axes[1].set_title('Price Box Plot')
    axes[1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300)
    logger.info(f"Visualization saved to {output_path}")
    plt.close()


def main():
    """Main analysis workflow."""
    logger.info("Starting retail product analysis...")
    
    # Connect to database
    conn = connect_to_postgres()
    
    try:
        # Fetch data
        df = fetch_product_data(conn)
        
        # Display basic info
        logger.info(f"Dataset shape: {df.shape}")
        logger.info(f"Columns: {df.columns.tolist()}")
        
        # Price statistics
        price_stats = analyze_price_statistics(df)
        if price_stats:
            logger.info("Price Statistics:")
            for stat, value in price_stats.items():
                logger.info(f"  {stat}: {value}")
        
        # Product distribution
        distribution = analyze_product_distribution(df)
        if distribution:
            logger.info("Product Distribution:")
            for col, counts in distribution.items():
                logger.info(f"\n  Top 5 {col}:")
                logger.info(counts.head(5).to_string())
        
        # Create visualizations
        visualize_price_distribution(df)
        
        logger.info("Analysis completed successfully!")
        
    finally:
        conn.close()


if __name__ == "__main__":
    main()