import pandas as pd
import numpy as np
from olist.data import Olist
from olist.order import Order


class Seller:

    def __init__(self):
        # Import only data once
        olist = Olist()
        self.data = olist.get_data()
        self.matching_table = olist.get_matching_table()
        self.get_orders_datetime = Olist().get_orders_datetime()
        self.order = Order()

    def get_seller_features(self):
        """
        Returns a DataFrame with:
       'seller_id', 'seller_city', 'seller_state'
        """
        sellers = self.data['sellers'].copy()
        sellers.drop('seller_zip_code_prefix', axis=1, inplace=True)
        # There are multiple rows per seller
        sellers.drop_duplicates(inplace=True)
        return sellers

    def get_seller_delay_wait_time(self):
        """
        Returns a DataFrame with:
       'seller_id', 'delay_to_carrier', 'seller_wait_time'
        """
        
        orders = self.get_orders_datetime.query("order_status=='delivered'").copy()
        order_items = self.data["order_items"].copy()
        orders = orders.merge(order_items, how="outer").merge(self.get_seller_features(), on = "seller_id", how="outer")
        orders['shipping_limit_date'] = pd.to_datetime(orders['shipping_limit_date'])
        orders['delay_to_carrier'] = ((orders['order_delivered_customer_date'] - orders['shipping_limit_date']) / np.timedelta64(1,'D')).apply(lambda x: max(x,0))
        orders['wait_time'] = ((orders['order_delivered_customer_date'] - orders['order_purchase_timestamp']) / np.timedelta64(1,'D')).apply(lambda x: max(x,0))
        sellers = orders[["seller_id", "delay_to_carrier", "wait_time"]].groupby("seller_id").mean().dropna()
        return sellers.reset_index()
        
    def get_active_dates(self):
        """
        Returns a DataFrame with:
       'seller_id', 'date_first_sale', 'date_last_sale'
        """
        orders = self.get_orders_datetime.query("order_status=='delivered'").copy()
        order_items = self.data["order_items"]
        orders = orders.merge(order_items, how="outer").merge(self.get_seller_features(), on = "seller_id", how="outer")
        orders = orders[["seller_id", "order_approved_at", "order_id"]].dropna()
        orders.loc[:,"date_first_sale"] = pd.to_datetime(orders['order_approved_at'])
        orders["date_last_sale"] = orders["date_first_sale"]
        orders.head()
        orders = orders.groupby("seller_id").agg({
            "date_first_sale": min,
            "date_last_sale": max
            }).reset_index()
        return orders[["seller_id", 'date_first_sale', 'date_last_sale']]

    def get_review_score(self):
        """
        Returns a DataFrame with:
        'seller_id', 'share_of_five_stars', 'share_of_one_stars',
        'review_score'
        """
        matching_table = self.matching_table
        orders_reviews = self.order.get_review_score()

        # Since the same seller can appear multiple times in the same order,
        # create a (seller <> order) matching table

        matching_table = matching_table[['order_id', 'seller_id']]\
            .drop_duplicates()
        reviews_df = matching_table.merge(orders_reviews, on='order_id')
        reviews_df = reviews_df.groupby(
            'seller_id', as_index=False).agg({'dim_is_one_star': 'mean',
                                              'dim_is_five_star': 'mean',
                                              'review_score': 'mean'})
        # Rename columns
        reviews_df.columns = ['seller_id', 'share_of_one_stars',
                              'share_of_five_stars', 'review_score']

        return reviews_df

    def get_quantity(self):
        """
        Returns a DataFrame with:
        'seller_id', 'n_orders', 'quantity', 'quantity_per_order'
        """
        order_items = self.data['order_items']

        n_orders = order_items.groupby('seller_id')['order_id']\
            .nunique().reset_index()
        n_orders.columns = ['seller_id', 'n_orders']

        quantity = order_items.groupby('seller_id', as_index=False)\
            .agg({'order_id': 'count'})
        quantity.columns = ['seller_id', 'quantity']

        result = n_orders.merge(quantity, on='seller_id')
        result['quantity_per_order'] = result['quantity'] / result['n_orders']
        return result

    def get_sales(self):
        """
        Returns a DataFrame with:
        'seller_id', 'sales'
        """
        return self.data['order_items'][['seller_id', 'price']]\
            .groupby('seller_id')\
            .sum()\
            .rename(columns={'price': 'sales'})

    def get_training_data(self):
        """
        Returns a DataFrame with:
        'seller_id', 'seller_state', 'seller_city', 'delay_to_carrier',
        'seller_wait_time', 'share_of_five_stars', 'share_of_one_stars',
        'seller_review_score', 'n_orders', 'quantity', 'date_first_sale', 'date_last_sale', 'sales'
        """
        training_set =\
            self.get_seller_features()\
                .merge(
                self.get_seller_delay_wait_time(), on='seller_id'
               ).merge(
                self.get_active_dates(), on='seller_id'
               ).merge(
                self.get_review_score(), on='seller_id'
               ).merge(
                self.get_quantity(), on='seller_id'
               ).merge(
                self.get_sales(), on='seller_id'
               )

        return training_set


