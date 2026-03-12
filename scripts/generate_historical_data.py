#!/usr/bin/env python3
"""
Generate 5 years of historical data for volume projection testing.
Includes progress bar and optimized price caching.
"""

from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import random
from tqdm import tqdm

# Database connection
DB_URL = "mssql+pyodbc://pioneertest:mango1234!@pioneertest.database.windows.net:1433/free-sql-db-3300567?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes&timeout=30"

def main():
    engine = create_engine(DB_URL)
    
    print("=== Generating 5 Years of Historical Data ===\n")
    
    start_date = datetime.now() - timedelta(days=5*365)
    end_date = datetime.now()
    
    with engine.begin() as conn:
        # Delete in correct order
        print("Clearing existing data...")
        delete_order = [
            'sales_order_items', 'purchase_order_items',
            'sales_orders', 'purchase_orders',
            'inventory',
            'employees', 'products',
            'customers', 'suppliers', 'stores'
        ]
        for table in tqdm(delete_order, desc="Clearing tables"):
            try:
                conn.execute(text(f"DELETE FROM [dbo].[{table}]"))
            except Exception as e:
                print(f"  Warning: Could not clear {table}: {e}")
        
        print("\nGenerating historical data...\n")
        
        # 1. Suppliers
        print("Inserting suppliers...")
        supplier_ids = []
        for i in tqdm(range(1, 11), desc="Suppliers"):
            days_ago = random.randint(0, 3*365)
            created_at = start_date + timedelta(days=days_ago)
            result = conn.execute(text("""
                INSERT INTO [dbo].[suppliers] (name, email, phone, created_at, updated_at)
                OUTPUT INSERTED.supplier_id
                VALUES (:name, :email, :phone, :created, :updated)
            """), {
                'name': f'Supplier {i}',
                'email': f'supplier{i}@example.com',
                'phone': f'555-{2000+i:04d}',
                'created': created_at,
                'updated': created_at
            })
            supplier_ids.append(result.scalar())
        print(f"  ✓ Inserted {len(supplier_ids)} suppliers\n")
        
        # 2. Stores
        print("Inserting stores...")
        store_ids = []
        store_codes = ['DTM', 'MAL', 'AIR', 'SUB', 'ONL']
        store_names_list = ['Downtown Market', 'Mall Store', 'Airport Shop', 'Suburb Store', 'Online Store']
        for i, (code, name) in enumerate(tqdm(zip(store_codes, store_names_list), total=len(store_codes), desc="Stores")):
            days_ago = random.randint(0, 4*365)
            created_at = start_date + timedelta(days=days_ago)
            result = conn.execute(text("""
                INSERT INTO [dbo].[stores] (name, code, address, city, state, postal_code, phone, created_at, updated_at)
                OUTPUT INSERTED.store_id
                VALUES (:name, :code, :addr, :city, :state, :postal, :phone, :created, :updated)
            """), {
                'name': name,
                'code': code,
                'addr': f'{i+1} Main St',
                'city': random.choice(['Dublin', 'Cork', 'Galway', 'Limerick']),
                'state': random.choice(['Leinster', 'Munster', 'Connacht']),
                'postal': f'{10000+i:05d}',
                'phone': f'555-{3000+i:04d}',
                'created': created_at,
                'updated': created_at
            })
            store_ids.append(result.scalar())
        print(f"  ✓ Inserted {len(store_ids)} stores\n")
        
        # 3. Customers
        print("Inserting customers...")
        customer_ids = []
        for i in tqdm(range(1, 101), desc="Customers"):
            days_ago = random.randint(0, 5*365)
            created_at = start_date + timedelta(days=days_ago)
            result = conn.execute(text("""
                INSERT INTO [dbo].[customers] (first_name, last_name, email, phone, created_at, updated_at)
                OUTPUT INSERTED.customer_id
                VALUES (:first, :last, :email, :phone, :created, :updated)
            """), {
                'first': f'Customer{i}',
                'last': f'LastName{i}',
                'email': f'customer{i}@example.com',
                'phone': f'555-{1000+i:04d}',
                'created': created_at,
                'updated': created_at
            })
            customer_ids.append(result.scalar())
        print(f"  ✓ Inserted {len(customer_ids)} customers\n")
        
        # 4. Products (cache prices for later use)
        print("Inserting products...")
        product_ids = []
        product_prices = {}  # Cache for faster lookups
        categories = ['Electronics', 'Clothing', 'Home', 'Sports', 'Books']
        for i in tqdm(range(1, 51), desc="Products"):
            days_ago = random.randint(0, 4*365)
            created_at = start_date + timedelta(days=days_ago)
            unit_price = round(random.uniform(10, 500), 2)
            cost_price = round(unit_price * random.uniform(0.5, 0.8), 2)
            result = conn.execute(text("""
                INSERT INTO [dbo].[products] (supplier_id, sku, name, category, unit_price, cost_price, active, created_at, updated_at, primary_supplier_id)
                OUTPUT INSERTED.product_id
                VALUES (:supplier_id, :sku, :name, :category, :unit_price, :cost_price, :active, :created, :updated, :primary_supplier)
            """), {
                'supplier_id': random.choice(supplier_ids),
                'sku': f'SKU-{i:04d}',
                'name': f'Product {i}',
                'category': random.choice(categories),
                'unit_price': unit_price,
                'cost_price': cost_price,
                'active': 1,
                'created': created_at,
                'updated': created_at,
                'primary_supplier': random.choice(supplier_ids)
            })
            pid = result.scalar()
            product_ids.append(pid)
            product_prices[pid] = {'unit_price': unit_price, 'cost_price': cost_price}
        print(f"  ✓ Inserted {len(product_ids)} products\n")
        
        # 5. Employees
        print("Inserting employees...")
        employee_ids = []
        roles = ['Manager', 'Sales', 'Cashier', 'Stock']
        for i in tqdm(range(1, 21), desc="Employees"):
            days_ago = random.randint(0, 4*365)
            hire_date = start_date + timedelta(days=days_ago)
            created_at = hire_date
            result = conn.execute(text("""
                INSERT INTO [dbo].[employees] (store_id, first_name, last_name, email, role, hire_date, created_at, updated_at)
                OUTPUT INSERTED.employee_id
                VALUES (:store_id, :first, :last, :email, :role, :hire_date, :created, :updated)
            """), {
                'store_id': random.choice(store_ids),
                'first': f'Employee{i}',
                'last': f'LastName{i}',
                'email': f'emp{i}@example.com',
                'role': random.choice(roles),
                'hire_date': hire_date.date(),
                'created': created_at,
                'updated': created_at
            })
            employee_ids.append(result.scalar())
        print(f"  ✓ Inserted {len(employee_ids)} employees\n")
        
        # 6. Sales Orders (distributed over 5 years with growth trend)
        print("Inserting sales orders...")
        sales_order_ids = []
        order_num = 1
        total_orders = sum(20 + (year * 10) for year in range(5))  # Total: 200 orders
        pbar = tqdm(total=total_orders, desc="Sales Orders")
        
        for year in range(5):
            year_start = start_date + timedelta(days=year*365)
            orders_this_year = 20 + (year * 10)  # Growth: 20, 30, 40, 50, 60
            
            for _ in range(orders_this_year):
                days_offset = random.randint(0, 365)
                order_date = year_start + timedelta(days=days_offset)
                if order_date > end_date:
                    pbar.update(1)
                    continue
                result = conn.execute(text("""
                    INSERT INTO [dbo].[sales_orders] (customer_id, store_id, employee_id, order_date, status, total_amount, created_at, updated_at)
                    OUTPUT INSERTED.sales_order_id
                    VALUES (:customer_id, :store_id, :employee_id, :order_date, :status, :total, :created, :updated)
                """), {
                    'customer_id': random.choice(customer_ids[:min(100, order_num)]),
                    'store_id': random.choice(store_ids),
                    'employee_id': random.choice(employee_ids),
                    'order_date': order_date,
                    'status': random.choice(['Pending', 'Completed', 'Shipped']),
                    'total': round(random.uniform(50, 1000), 2),
                    'created': order_date,
                    'updated': order_date
                })
                sales_order_ids.append(result.scalar())
                order_num += 1
                pbar.update(1)
        pbar.close()
        print(f"  ✓ Inserted {len(sales_order_ids)} sales orders\n")
        
        # 7. Sales Order Items (use cached prices - much faster!)
        print("Inserting sales order items...")
        total_items = sum(random.randint(1, 4) for _ in sales_order_ids)
        pbar = tqdm(total=len(sales_order_ids), desc="Sales Order Items")
        
        for sales_order_id in sales_order_ids:
            num_items = random.randint(1, 4)
            selected_products = random.sample(product_ids, min(num_items, len(product_ids)))
            for product_id in selected_products:
                unit_price = product_prices[product_id]['unit_price']
                conn.execute(text("""
                    INSERT INTO [dbo].[sales_order_items] (sales_order_id, product_id, quantity, unit_price, created_at, updated_at)
                    VALUES (:sales_order_id, :product_id, :qty, :unit_price, :created, :updated)
                """), {
                    'sales_order_id': sales_order_id,
                    'product_id': product_id,
                    'qty': random.randint(1, 5),
                    'unit_price': unit_price,
                    'created': datetime.now(),
                    'updated': datetime.now()
                })
            pbar.update(1)
        pbar.close()
        result = conn.execute(text("SELECT COUNT(*) FROM [dbo].[sales_order_items]"))
        print(f"  ✓ Inserted {result.scalar()} sales order items\n")
        
        # 8. Purchase Orders
        print("Inserting purchase orders...")
        purchase_order_ids = []
        po_num = 1
        total_pos = sum(15 + (year * 5) for year in range(5))  # Total: 100 orders
        pbar = tqdm(total=total_pos, desc="Purchase Orders")
        
        for year in range(5):
            year_start = start_date + timedelta(days=year*365)
            pos_this_year = 15 + (year * 5)  # Growth trend
            
            for _ in range(pos_this_year):
                days_offset = random.randint(0, 365)
                order_date = year_start + timedelta(days=days_offset)
                if order_date > end_date:
                    pbar.update(1)
                    continue
                expected_date = order_date + timedelta(days=random.randint(7, 30))
                result = conn.execute(text("""
                    INSERT INTO [dbo].[purchase_orders] (supplier_id, store_id, status, order_date, expected_date, created_at, updated_at)
                    OUTPUT INSERTED.po_id
                    VALUES (:supplier_id, :store_id, :status, :order_date, :expected_date, :created, :updated)
                """), {
                    'supplier_id': random.choice(supplier_ids),
                    'store_id': random.choice(store_ids),
                    'status': random.choice(['Pending', 'Ordered', 'Received']),
                    'order_date': order_date.date(),
                    'expected_date': expected_date.date(),
                    'created': order_date,
                    'updated': order_date
                })
                purchase_order_ids.append(result.scalar())
                po_num += 1
                pbar.update(1)
        pbar.close()
        print(f"  ✓ Inserted {len(purchase_order_ids)} purchase orders\n")
        
        # 9. Purchase Order Items (use cached prices)
        print("Inserting purchase order items...")
        pbar = tqdm(total=len(purchase_order_ids), desc="Purchase Order Items")
        
        for po_id in purchase_order_ids:
            num_items = random.randint(1, 3)
            selected_products = random.sample(product_ids, min(num_items, len(product_ids)))
            for product_id in selected_products:
                cost_price = product_prices[product_id]['cost_price']
                conn.execute(text("""
                    INSERT INTO [dbo].[purchase_order_items] (po_id, product_id, quantity, unit_cost, created_at, updated_at)
                    VALUES (:po_id, :product_id, :qty, :unit_cost, :created, :updated)
                """), {
                    'po_id': po_id,
                    'product_id': product_id,
                    'qty': random.randint(10, 50),
                    'unit_cost': cost_price,
                    'created': datetime.now(),
                    'updated': datetime.now()
                })
            pbar.update(1)
        pbar.close()
        result = conn.execute(text("SELECT COUNT(*) FROM [dbo].[purchase_order_items]"))
        print(f"  ✓ Inserted {result.scalar()} purchase order items\n")
        
        # 10. Inventory (FIXED: includes reorder_level)
        print("Inserting inventory...")
        total_inventory = len(store_ids) * min(20, len(product_ids))
        pbar = tqdm(total=total_inventory, desc="Inventory")
        
        for store_id in store_ids:
            for product_id in random.sample(product_ids, min(20, len(product_ids))):
                days_ago = random.randint(0, 2*365)
                restocked_at = start_date + timedelta(days=3*365 + days_ago)
                created_at = restocked_at - timedelta(days=random.randint(0, 30))
                qty = random.randint(0, 100)
                reorder_level = random.randint(10, 30)  # Required field
                conn.execute(text("""
                    INSERT INTO [dbo].[inventory] (store_id, product_id, quantity_on_hand, reorder_level, last_restocked_at, created_at, updated_at)
                    VALUES (:store_id, :product_id, :qty, :reorder_level, :restocked, :created, :updated)
                """), {
                    'store_id': store_id,
                    'product_id': product_id,
                    'qty': qty,
                    'reorder_level': reorder_level,
                    'restocked': restocked_at,
                    'created': created_at,
                    'updated': restocked_at
                })
                pbar.update(1)
        pbar.close()
        result = conn.execute(text("SELECT COUNT(*) FROM [dbo].[inventory]"))
        print(f"  ✓ Inserted {result.scalar()} inventory records\n")
    
    print("\n" + "="*50)
    print("✅ Data Generation Complete!")
    print("="*50)
    print("\nHistorical data spanning 5 years has been inserted.")
    print("You can now run the volume projection collector to generate meaningful projections.")

if __name__ == "__main__":
    main()
