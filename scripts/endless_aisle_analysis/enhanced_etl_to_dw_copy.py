import os
import sqlite3
import pandas as pd
import logging

# ——— Configuration —————————————————————————————————————————
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
BASE_DIR = r"C:\Projects\smart-store-kiruthikaa"
DATA_DIR = os.path.join(BASE_DIR, "data", "cleaned")
DW_PATH = os.path.join(BASE_DIR, "data", "dw", "smart_sales.db")

BRAND_RULES = {
    "HCL": "HCL",
    "Bloom": "BloomWear",
    "NKT": "NKT Tech",
    "Haptics": "Haptics Co.",
    "Wilson": "Wilson Sports"
}
# ——————————————————————————————————————————————————————————————————


def enhance_customer(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Enhancing customer data…")
    df.columns = df.columns.str.strip().str.lower()
    # Fill defaults
    df["joindate"] = df.get("joindate", pd.Series(["2023-01-01"] * len(df))).fillna("2023-01-01")
    df["loyaltypoints"] = (
        pd.to_numeric(df.get("loyaltypoints", 0), errors="coerce")
        .fillna(0)
        .astype(int)
    )
    df["preferredcontactmethod"] = df.get(
        "preferredcontactmethod",
        pd.Series(["Email"] * len(df))
    ).fillna("Email")
    # Cast to str
    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].astype(str)
    logging.info("✔ Customer enhanced: %d rows", len(df))
    return df.drop_duplicates()


def enhance_product(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Enhancing product data…")
    df.columns = df.columns.str.strip().str.lower()
    # Synthetic columns
    df["productcategory"] = df.get(
        "productcategory",
        pd.Series(["General"] * len(df))
    ).fillna("General")
    df["stockstatus"] = df.get(
        "stockstatus",
        pd.Series(["InStock"] * len(df))
    ).fillna("InStock")
    # Brand from supplier
    df["supplier"] = df.get("supplier", "").astype(str).str.strip().str.upper()
    df["brand"] = df["supplier"].map(BRAND_RULES).fillna("Generic")
    # Cast to str
    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].astype(str)
    logging.info("✔ Product enhanced: %d rows", len(df))
    return df.drop_duplicates()


def enhance_sales(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Enhancing sales data…")
    df.columns = df.columns.str.strip().str.lower()
    
    # Numeric coercion
    df["saleamount"] = pd.to_numeric(df.get("saleamount", 0), errors="coerce").fillna(0)
    df["bonuspoints"] = pd.to_numeric(df.get("bonuspoints", 0), errors="coerce").fillna(0)
    
    # Discount percent
    df["discountpercent"] = (
        (df["bonuspoints"] / df["saleamount"].replace(0, pd.NA) * 100)
        .round(2)
        .fillna(0.0)
    )
    
    # Fulfillment - only calculate if column doesn't exist
    if "fulfillmenttype" not in df.columns:
        df["transactionid"] = df.get("transactionid", "").astype(str)
        df["fulfillmenttype"] = df["transactionid"].str.upper().apply(
            lambda x: "Endless Aisle" if x.startswith("EA") else "POS"
        )
    
    # Repeat customer - only calculate if column doesn't exist
    if "isrepeatcustomer" not in df.columns:
        df["isrepeatcustomer"] = df.duplicated(subset="customerid", keep=False).astype(int)
    else:
        # Convert TRUE/FALSE to 1/0 if needed
        df["isrepeatcustomer"] = df["isrepeatcustomer"].astype(bool).astype(int)
    
    # Anomaly flag - only calculate if column doesn't exist
    if "anomalyflag" not in df.columns:
        mean = df["saleamount"].mean()
        std = df["saleamount"].std()
        thresh = mean + 2 * std
        df["anomalyflag"] = (df["saleamount"] > thresh).astype(int)
    else:
        # Convert TRUE/FALSE to 1/0 if needed
        df["anomalyflag"] = df["anomalyflag"].astype(bool).astype(int)
    
    # Cast to str
    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].astype(str)
    
    logging.info("✔ Sales enhanced: %d rows", len(df))
    return df.drop_duplicates()


def reset_and_create_dw(conn: sqlite3.Connection):
    logging.info("Resetting DW schema…")
    c = conn.cursor()
    c.executescript("""
    DROP TABLE IF EXISTS customer;
    CREATE TABLE customer (
      customer_id INTEGER PRIMARY KEY,
      name TEXT,
      region TEXT,
      join_date TEXT NOT NULL,
      loyalty_points INTEGER,
      preferred_contact_method TEXT
    );

    DROP TABLE IF EXISTS product;
    CREATE TABLE product (
      product_id INTEGER PRIMARY KEY,
      product_name TEXT,
      category TEXT,
      unit_price REAL,
      stock_quantity INTEGER,
      supplier TEXT,
      product_category TEXT,
      brand TEXT,
      stock_status TEXT
    );

    DROP TABLE IF EXISTS sale;
    CREATE TABLE sale (
      sale_id TEXT PRIMARY KEY,
      sale_date TEXT,
      customer_id INTEGER,
      product_id INTEGER,
      store_id INTEGER,
      campaign_id INTEGER,
      sale_amount REAL,
      bonus_points REAL,
      payment_type TEXT,
      discount_percent REAL,
      fulfillment_type TEXT,
      is_repeat_customer INTEGER,
      anomaly_flag INTEGER
    );
    """)
    conn.commit()
    logging.info("✅ DW schema ready.")


def load_df_to_dw(conn: sqlite3.Connection, df: pd.DataFrame, tbl: str, rename_map: dict):
    logging.info(f"Loading `{tbl}`…")
    df = df.rename(columns=rename_map)
    # drop any cols not in table
    cols = pd.read_sql(f"PRAGMA table_info({tbl})", conn)["name"].tolist()
    df = df[[c for c in df.columns if c in cols]]
    df.to_sql(tbl, conn, if_exists="append", index=False)
    logging.info(f"✅ Loaded `{tbl}` ({len(df)} rows).")


def generate_sales_summary(df: pd.DataFrame):
    # Use transactionid instead of saleid which doesn't exist
    transaction_id_col = "transactionid" if "transactionid" in df.columns else "sale_id"
    
    # Anomaly Detection - Sales Amount
    mean = df["saleamount"].mean()
    std = df["saleamount"].std()
    threshold = mean + 2 * std
    df["anomaly_flag"] = (df["saleamount"] > threshold).astype(int)

    # Summary of Anomalous Sales
    anomalous_sales = df[df["anomaly_flag"] == 1]

    # Repeat Customer Summary - use transactionid or sale_id
    repeat_customers = df.groupby("customerid")[transaction_id_col].count()
    repeat_customers = repeat_customers[repeat_customers > 1]

    # Campaign Summary (example)
    campaign_summary = df.groupby('campaignid').agg(
        total_sales=pd.NamedAgg(column='saleamount', aggfunc='sum'),
        avg_discount=pd.NamedAgg(column='discountpercent', aggfunc='mean'),
        repeat_customers=pd.NamedAgg(column='isrepeatcustomer', aggfunc='sum')
    )

    # Print Summary
    logging.info(f"Total Anomalous Sales: {len(anomalous_sales)}")
    logging.info(f"Total Repeat Customers: {len(repeat_customers)}")
    logging.info("Campaign Summary:")
    logging.info(campaign_summary)


def main():
    logging.info("▶️  ETL starting…")
    # 1) Read cleaned CSVs
    cust = enhance_customer(pd.read_csv(os.path.join(DATA_DIR, "enhanced_customer_data.csv")))
    prod = enhance_product(pd.read_csv(os.path.join(DATA_DIR, "enhanced_product_data.csv")))
    sale = enhance_sales(pd.read_csv(os.path.join(DATA_DIR, "enhanced_sale_data.csv")))

    # 2) Generate Sales Summary
    generate_sales_summary(sale)

    # 3) Build DW
    os.makedirs(os.path.dirname(DW_PATH), exist_ok=True)
    if os.path.exists(DW_PATH):
        os.remove(DW_PATH)
    conn = sqlite3.connect(DW_PATH)
    try:
        reset_and_create_dw(conn)
        # 4) Load Data into DW
        load_df_to_dw(conn, cust, "customer", {
            "customerid": "customer_id",
            "name": "name",
            "region": "region",
            "joindate": "join_date",
            "loyaltypoints": "loyalty_points",
            "preferredcontactmethod": "preferred_contact_method"
        })
        load_df_to_dw(conn, prod, "product", {
            "productid": "product_id",
            "productname": "product_name",
            "category": "category",
            "unitprice": "unit_price",
            "stockquantity": "stock_quantity",
            "supplier": "supplier",
            "productcategory": "product_category",
            "brand": "brand",
            "stockstatus": "stock_status"
        })
        load_df_to_dw(conn, sale, "sale", {
            "transactionid": "sale_id",
            "saledate": "sale_date",
            "customerid": "customer_id",
            "productid": "product_id",
            "storeid": "store_id",
            "campaignid": "campaign_id",
            "saleamount": "sale_amount",
            "bonuspoints": "bonus_points",
            "paymenttype": "payment_type",
            "discountpercent": "discount_percent",
            "fulfillmenttype": "fulfillment_type",
            "isrepeatcustomer": "is_repeat_customer",
            "anomalyflag": "anomaly_flag"
        })
    finally:
        conn.close()
        logging.info("🎉 ETL complete — DW ready at `%s`", DW_PATH)

if __name__ == "__main__":
    main()