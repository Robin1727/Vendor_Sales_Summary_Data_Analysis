import sqlite3
import pandas as pd
import logging
from Inventory_ingestion import ingest_db

# ---------------- LOGGING SETUP ----------------
logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.debug,
    format="%(asctime)s-%(levelname)s-%(message)s",
    filemode="a"
)

#----------------Create Vendor_summary table -----------

def create_vendor_summary(conn):
    '''this function will merge the different tables to get the overall vendor summary and adding new columns in the resultant data'''
    
    vendor_sales_summary = pd.read_sql_query('''
    WITH Freight_summary AS (
        SELECT 
            VendorNumber, 
            SUM(Freight) AS FreightCost 
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),
    Purchase_summary AS (
        SELECT 
            p.VendorNumber, 
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars,
            pp.Volume,
            pp.Price AS ActualPrice
        FROM purchases p
        JOIN Purchase_prices pp 
            ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY p.VendorNumber, p.VendorName, p.Brand
    ),
    Sales_summary AS (
        SELECT
            VendorNo,
            Brand,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(SalesPrice) AS TotalSalesPrice,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(ExciseTax) AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )
    SELECT 
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,
        fs.FreightCost
    FROM Purchase_summary ps
    LEFT JOIN Sales_summary ss
        ON ps.VendorNumber = ss.VendorNo
        AND ps.Brand = ss.Brand
    LEFT JOIN Freight_summary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC
    ''', conn)
    return vendor_sales_summary
#----------------Creating function for data cleaning -----------

    
def clean_data(df):
    '''this function will clean data'''
    # changing datatype to float
    df["Volume"] = df["Volume"].astype('float64')

    # filling missing value with 0
    df.fillna(0, inplace = True)

    # Removing spaces from categorical columns
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()
    
    # Creating new columns for better analysis
    vendor_sales_summary['GrossProfit'] = vendor_sales_summary['TotalSalesDollars'] - vendor_sales_summary['TotalPurchaseDollars']
    vendor_sales_summary['ProfitMargin'] = (vendor_sales_summary['GrossProfit']/vendor_sales_summary['TotalSalesDollars'])*100
    vendor_sales_summary['StockTurnover'] = vendor_sales_summary['TotalSalesQuantity']/vendor_sales_summary['TotalPurchaseQuantity']
    vendor_sales_summary['SalesToPurchaseRatio'] = vendor_sales_summary['TotalSalesDollars']/vendor_sales_summary['TotalPurchaseDollars']
    
    return df

#----------------Calling ingest function-----------


if __name__ == '__main__':
    #creating database connection
    conn=sqlite3.connect('inventory.db')
    
    logging.info('Creating vendor summary table...')
    summary_df= create_vendor_summary(conn)
    logging.info(summary_df.head())
    
    logging.info('cleaning data...')
    clean_df= clean_data(summary_df)
    logging.info(clean_df.head())
    
    logging.info('Ingesting data...')
    ingest_db(clean_df,'vendor_sales_summary',conn)
    logging.info('completed')
