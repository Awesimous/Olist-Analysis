import os
import pandas as pd


class Olist:

    def get_data(self):
        """
        This function returns a Python dict.
        Its keys should be 'sellers', 'orders', 'order_items' etc...
        Its values should be pandas.DataFrame loaded from csv files
        """
        # Hints: Build csv_path as "absolute path" in order to call this method from anywhere.
        # Do not hardcode your path as it only works on your machine ('Users/username/code...')
        # Use __file__ as absolute path anchor independant of your computer
        # Make extensive use of `import ipdb; ipdb.set_trace()` to investigate what `__file__` variable is really
        # Use os.path library to construct path independent of Unix vs. Windows specificities
        # refers the relative path of module.py
        csv_path = os.path.abspath(os.path.join(__file__, '../../data/csv/'))
        file_names = os.listdir(csv_path)
        file_names.remove(".gitkeep")
        key_names = [str(i).replace('.csv', "").replace("olist_", "").replace("_dataset", "") for i in file_names]
        csv_df = [pd.read_csv(os.path.join(csv_path, ele)) for ele in file_names]
        data = {}
        for x,y in zip(key_names, csv_df):
            data[x] = y
        return data

    def get_orders_datetime(self):
        """
        01-01 > This function transfers all date colums to datetime 
        """
        data = self.get_data()
        orders = data["orders"]
        orders['order_delivered_carrier_date'] = pd.to_datetime(orders['order_delivered_carrier_date'])
        orders['order_estimated_delivery_date'] = pd.to_datetime(orders['order_estimated_delivery_date'])
        orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])
        orders['order_approved_at'] = pd.to_datetime(orders['order_approved_at'])
        orders['order_delivered_customer_date'] = pd.to_datetime(orders['order_delivered_customer_date'])
        return orders
    
    def get_matching_table(self):
        """
        01-01 > This function returns a matching table between
        columns [ "order_id", "review_id", "customer_id", "product_id", "seller_id"]
        """
        data = self.get_data() 
        order_id = data["orders"].filter(items=["order_id","customer_id"])
        review_id = data["order_reviews"].filter(items=["review_id","order_id"])
        customer_id = data["customers"].filter(items=["customer_id"])
        product_id = data["products"].filter(items=["product_id"])
        seller_id = data["sellers"]["seller_id"]
        order_items = data["order_items"].filter(items=["order_id", "product_id", "seller_id"])
        matching_table = order_id.merge(review_id, how="outer").merge(customer_id,how="outer").merge(order_items,how="outer").merge(product_id,how="outer").merge(seller_id,how="outer")
        return matching_table
        
    def ping(self):
        """
        You call ping I print pong.
        """
        print('pong')
