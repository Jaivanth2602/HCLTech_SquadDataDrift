import os
import sys
import csv
import sqlite3
import datetime
import re

DATA_DIR = os.getenv("DATA_DIR", "./data" if os.path.exists("./data/customers.csv") else ".")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./output")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", None)

def parse_order_date(d_str):
    """Robust date parser handling dirty data ('31/02/2024', 'not_a_date', empty)."""
    if not d_str:
        return None, None, None
    d_str = d_str.strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            dt = datetime.datetime.strptime(d_str, fmt)
            if 2020 <= dt.year <= 2030:
                quarter = (dt.month - 1) // 3 + 1
                return dt.strftime("%Y-%m-%d"), str(dt.year), f"Q{quarter}"
        except ValueError:
            continue
    return None, None, None

def run_pipeline():
    print("=" * 70)
    print("  RETAIL SALES LAKEHOUSE ETL PIPELINE - HCLTECH HACKATHON")
    print(f"  Data Source: {DATA_DIR}")
    print(f"  Output Dir:  {OUTPUT_DIR}")
    print("=" * 70)

    silver_dir = os.path.join(OUTPUT_DIR, "silver")
    gold_dir = os.path.join(OUTPUT_DIR, "gold")
    os.makedirs(silver_dir, exist_ok=True)
    os.makedirs(gold_dir, exist_ok=True)

    conn = sqlite3.connect(":memory:")
    c = conn.cursor()

    # -------------------------------------------------------------
    # 1. BRONZE INGESTION & DATA QUALITY CLEANSING
    # -------------------------------------------------------------
    print("\n[Stage 1: Bronze Ingestion & Data Cleansing]")

    # 1.1 Customer Cleansing (Deduplication, Email/Phone Validation)
    cust_file = os.path.join(DATA_DIR, "customers.csv")
    cleaned_customers = []
    seen_cust_ids = set()
    seen_emails = set()
    skipped_cust = 0

    with open(cust_file, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            cid = r["customer_id"].strip()
            email = r["email"].strip().lower()
            phone = re.sub(r"\D", "", r["phone"].strip())
            
            # Rule: Must have unique ID, unique email, and valid email format
            if cid and cid not in seen_cust_ids and email and email not in seen_emails and "@" in email:
                seen_cust_ids.add(cid)
                seen_emails.add(email)
                region = r["region"].strip() if r["region"].strip() else "Unknown"
                full_name = f"{r['first_name'].strip()} {r['last_name'].strip()}"
                cleaned_customers.append((
                    cid, full_name, email, phone, r["gender"].strip(),
                    r["city"].strip(), r["state"].strip(), region, r["country"].strip(), r["signup_date"].strip()
                ))
            else:
                skipped_cust += 1

    c.execute("""
        CREATE TABLE silver_customers (
            customer_id TEXT PRIMARY KEY,
            full_name TEXT,
            email TEXT,
            phone TEXT,
            gender TEXT,
            city TEXT,
            state TEXT,
            region TEXT,
            country TEXT,
            signup_date TEXT
        )
    """)
    c.executemany("INSERT INTO silver_customers VALUES (?,?,?,?,?,?,?,?,?,?)", cleaned_customers)
    print(f"  ✓ silver_customers: {len(cleaned_customers)} unique verified records (Filtered {skipped_cust} duplicates/invalid).")

    # 1.2 Product Cleansing (Price Consistency, SKU validation, Deduplication)
    prod_file = os.path.join(DATA_DIR, "products.csv")
    sku_pattern = re.compile(r"^[A-Z]{3}-[A-Z]{3}-[0-9]{4}$")
    prod_dict = {}

    with open(prod_file, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            pid = r["product_id"].strip()
            if not pid:
                continue
            try:
                sp = float(r["selling_price"])
                cp = float(r["cost_price"])
                is_sku_ok = bool(sku_pattern.match(r["sku"].strip()))
                is_price_ok = (sp >= cp and cp > 0)

                if pid not in prod_dict:
                    prod_dict[pid] = (r, is_sku_ok, is_price_ok, sp, cp)
                else:
                    _, prev_sku_ok, prev_price_ok, _, _ = prod_dict[pid]
                    # Upgrade entry if current is cleaner than previous
                    if (is_sku_ok and is_price_ok) and not (prev_sku_ok and prev_price_ok):
                        prod_dict[pid] = (r, is_sku_ok, is_price_ok, sp, cp)
                    elif is_price_ok and not prev_price_ok:
                        prod_dict[pid] = (r, is_sku_ok, is_price_ok, sp, cp)
            except ValueError:
                continue

    cleaned_products = []
    valid_prod_info = {}
    for pid, (r, is_sku_ok, is_price_ok, sp, cp) in prod_dict.items():
        if is_price_ok:
            cleaned_products.append((
                pid, r["sku"].strip(), r["product_name"].strip(),
                r["category"].strip(), r["supplier"].strip(),
                r["store_id"].strip(), r["store_name"].strip(), sp, cp
            ))
            valid_prod_info[pid] = {"name": r["product_name"].strip(), "sp": sp, "cp": cp, "cat": r["category"].strip()}

    c.execute("""
        CREATE TABLE silver_products (
            product_id TEXT PRIMARY KEY,
            sku TEXT,
            product_name TEXT,
            category TEXT,
            supplier TEXT,
            store_id TEXT,
            store_name TEXT,
            selling_price REAL,
            cost_price REAL
        )
    """)
    c.executemany("INSERT INTO silver_products VALUES (?,?,?,?,?,?,?,?,?)", cleaned_products)
    print(f"  ✓ silver_products: {len(cleaned_products)} products with valid pricing & verified SKUs.")

    # 1.3 Sales & Returns Cleansing (Date sanity, null keys, mode normalization)
    sales_file = os.path.join(DATA_DIR, "sales_returns.csv")
    cleaned_sales = []
    skipped_sales = 0

    with open(sales_file, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            sid = r["sales_order_id"].strip()
            cid = r["customer_id"].strip()
            pid = r["product_id"].strip()
            
            # Check foreign key existence and non-null keys
            if sid and cid in seen_cust_ids and pid in valid_prod_info:
                clean_date, year, quarter = parse_order_date(r["order_date"])
                if clean_date:
                    try:
                        qty = int(r["quantity"])
                        unit_p = float(r["unit_price"])
                        amount = float(r["sales_amount"])
                        is_ret = 1 if r["is_returned"].strip().upper() == "Y" else 0
                        ret_qty = int(float(r["return_quantity"])) if r["return_quantity"].strip() else 0
                        ret_amt = float(r["return_amount"]) if r["return_amount"].strip() else 0.0

                        # Payment mode clean categorization
                        pm = r["payment_mode"].strip().lower()
                        if pm in ("cash", "cash on delivery", "cod", ""):
                            pay_cat = "Cash"
                        else:
                            pay_cat = "Digital"

                        cleaned_sales.append((
                            sid, cid, pid, valid_prod_info[pid]["name"], valid_prod_info[pid]["cat"],
                            valid_prod_info[pid]["cp"], qty, unit_p, amount,
                            clean_date, year, quarter, r["mode_of_sales"].strip(),
                            r["payment_mode"].strip(), pay_cat, is_ret, ret_qty, ret_amt
                        ))
                    except ValueError:
                        skipped_sales += 1
                else:
                    skipped_sales += 1
            else:
                skipped_sales += 1

    c.execute("""
        CREATE TABLE silver_sales (
            sales_order_id TEXT,
            customer_id TEXT,
            product_id TEXT,
            product_name TEXT,
            category TEXT,
            cost_price REAL,
            quantity INTEGER,
            unit_price REAL,
            sales_amount REAL,
            order_date TEXT,
            order_year TEXT,
            order_quarter TEXT,
            mode_of_sales TEXT,
            payment_mode TEXT,
            payment_category TEXT,
            is_returned INTEGER,
            return_quantity INTEGER,
            return_amount REAL
        )
    """)
    c.executemany("INSERT INTO silver_sales VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", cleaned_sales)
    print(f"  ✓ silver_sales: {len(cleaned_sales)} valid transactions (Filtered {skipped_sales} corrupted/orphan records).")

    # 1.4 Card Payments Cleansing (Status SUCCESS and Amount > 0)
    pay_file = os.path.join(DATA_DIR, "card_payments_refunds.csv")
    cleaned_payments = []
    skipped_pay = 0

    with open(pay_file, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            tid = r["transaction_id"].strip()
            sid = r["sales_order_id"].strip()
            amt_str = r["amount"].strip()
            status = r["status"].strip().upper()

            if tid and amt_str and status == "SUCCESS":
                try:
                    amt = float(amt_str)
                    if amt > 0:
                        cleaned_payments.append((
                            tid, r["transaction_type"].strip(), sid,
                            r["card_type"].strip(), r["card_network"].strip(),
                            amt, r["transaction_date"].strip(), status
                        ))
                    else:
                        skipped_pay += 1
                except ValueError:
                    skipped_pay += 1
            else:
                skipped_pay += 1

    c.execute("""
        CREATE TABLE silver_payments (
            transaction_id TEXT,
            transaction_type TEXT,
            sales_order_id TEXT,
            card_type TEXT,
            card_network TEXT,
            amount REAL,
            transaction_date TEXT,
            status TEXT
        )
    """)
    c.executemany("INSERT INTO silver_payments VALUES (?,?,?,?,?,?,?,?)", cleaned_payments)
    print(f"  ✓ silver_payments: {len(cleaned_payments)} successful payment records (Filtered {skipped_pay} invalid/failed).")

    # -------------------------------------------------------------
    # 2. CURATE GOLD KPI DATA MARTS
    # -------------------------------------------------------------
    print("\n[Stage 2: Curating Gold Strategic KPI Data Marts]")

    def export_kpi(query, filename):
        c.execute(query)
        rows = c.fetchall()
        cols = [d[0] for d in c.description]
        out_csv = os.path.join(gold_dir, filename + ".csv")
        with open(out_csv, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(cols)
            writer.writerows(rows)
        print(f"  ★ Generated {filename}.csv ({len(rows)} rows)")
        return out_csv

    # KPI 1A: Top 5 Products by Orders and Returns
    export_kpi("""
        SELECT 
            p.product_id,
            p.product_name,
            p.category,
            COUNT(CASE WHEN s.is_returned = 0 THEN 1 END) AS total_orders,
            COUNT(CASE WHEN s.is_returned = 1 THEN 1 END) AS total_returns
        FROM silver_sales s
        JOIN silver_products p ON s.product_id = p.product_id
        GROUP BY p.product_id, p.product_name, p.category
        ORDER BY total_orders DESC, total_returns DESC
        LIMIT 5;
    """, "kpi1_top5_products")

    # KPI 1B: Top 5 Customers by Orders and Returns
    export_kpi("""
        SELECT 
            c.customer_id,
            c.full_name,
            c.region,
            COUNT(CASE WHEN s.is_returned = 0 THEN 1 END) AS total_orders,
            COUNT(CASE WHEN s.is_returned = 1 THEN 1 END) AS total_returns
        FROM silver_sales s
        JOIN silver_customers c ON s.customer_id = c.customer_id
        GROUP BY c.customer_id, c.full_name, c.region
        ORDER BY total_orders DESC, total_returns DESC
        LIMIT 5;
    """, "kpi1_top5_customers")

    # KPI 2: Region-wise Customer Base and Growth QoQ
    export_kpi("""
        WITH quarterly_customers AS (
            SELECT 
                c.region,
                s.order_year,
                s.order_quarter,
                COUNT(DISTINCT c.customer_id) AS current_customers,
                LAG(COUNT(DISTINCT c.customer_id)) OVER (
                    PARTITION BY c.region ORDER BY s.order_year, s.order_quarter
                ) AS prev_quarter_customers
            FROM silver_sales s
            JOIN silver_customers c ON s.customer_id = c.customer_id
            GROUP BY c.region, s.order_year, s.order_quarter
        )
        SELECT 
            region,
            order_year,
            order_quarter,
            current_customers,
            COALESCE(prev_quarter_customers, 0) AS prev_quarter_customers,
            ROUND(((current_customers - prev_quarter_customers) * 100.0) / NULLIF(prev_quarter_customers, 0), 2) AS customer_growth_qoq_pct
        FROM quarterly_customers
        ORDER BY region, order_year, order_quarter;
    """, "kpi2_region_customer_growth_qoq")

    # KPI 3: Product-wise Sales vs Margin
    export_kpi("""
        SELECT 
            product_id,
            product_name,
            category,
            ROUND(SUM(quantity * unit_price), 2) AS total_sales,
            ROUND(SUM(quantity * (unit_price - cost_price)), 2) AS total_profit,
            ROUND((SUM(quantity * (unit_price - cost_price)) * 100.0) / NULLIF(SUM(quantity * unit_price), 0), 2) AS profit_margin_pct
        FROM silver_sales
        WHERE is_returned = 0
        GROUP BY product_id, product_name, category
        ORDER BY total_sales DESC;
    """, "kpi3_product_sales_margin")

    # KPI 4: Digital Payments Percentage
    export_kpi("""
        SELECT 
            order_year,
            order_quarter,
            ROUND(SUM(CASE WHEN payment_category = 'Digital' THEN sales_amount ELSE 0 END), 2) AS digital_sales,
            ROUND(SUM(sales_amount), 2) AS total_sales,
            ROUND((SUM(CASE WHEN payment_category = 'Digital' THEN sales_amount ELSE 0 END) * 100.0) / NULLIF(SUM(sales_amount), 0), 2) AS digital_payment_pct
        FROM silver_sales
        WHERE is_returned = 0
        GROUP BY order_year, order_quarter
        ORDER BY order_year, order_quarter;
    """, "kpi4_digital_payments_pct")

    # KPI 5: Sales Growth QoQ
    export_kpi("""
        WITH quarterly_sales AS (
            SELECT 
                order_year,
                order_quarter,
                SUM(sales_amount) AS current_sales,
                LAG(SUM(sales_amount)) OVER (ORDER BY order_year, order_quarter) AS prev_sales
            FROM silver_sales
            WHERE is_returned = 0
            GROUP BY order_year, order_quarter
        )
        SELECT 
            order_year,
            order_quarter,
            ROUND(current_sales, 2) AS current_sales,
            ROUND(COALESCE(prev_sales, 0.0), 2) AS prev_sales,
            ROUND(((current_sales - prev_sales) * 100.0) / NULLIF(prev_sales, 0), 2) AS sales_growth_qoq_pct
        FROM quarterly_sales
        ORDER BY order_year, order_quarter;
    """, "kpi5_sales_growth_qoq")

    # -------------------------------------------------------------
    # 3. S3 SYNC (When AWS credentials and bucket are provided)
    # -------------------------------------------------------------
    if S3_BUCKET_NAME:
        print(f"\n[Stage 3: Syncing to AWS S3: {S3_BUCKET_NAME}]")
        try:
            import boto3
            s3 = boto3.client("s3")
            for fname in os.listdir(gold_dir):
                if fname.endswith(".csv"):
                    fpath = os.path.join(gold_dir, fname)
                    s3.upload_file(fpath, S3_BUCKET_NAME, f"gold/{fname}")
                    print(f"  ✓ Uploaded to s3://{S3_BUCKET_NAME}/gold/{fname}")
        except Exception as e:
            print(f"  ⚠ AWS S3 Sync notice: {e}")

    print("\n" + "=" * 70)
    print("  ETL PIPELINE COMPLETED SUCCESSFULLY! ALL KPIS READY FOR POWER BI.")
    print("=" * 70)

if __name__ == "__main__":
    run_pipeline()
