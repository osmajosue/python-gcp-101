import pandas as pd


class Transform:
    def top_sellers(self, order_items_df, products_df, categories_df, file_name):
        # merge order_items_df and products_df on order_item_product_id and product_id columns
        merged_df = order_items_df.merge(
            products_df,
            left_on="order_item_product_id",
            right_on="product_id",
            how="inner",
        )

        # merge merged_df and categories_df on category_id column
        merged_df = merged_df.merge(
            categories_df,
            left_on="product_category_id",
            right_on="category_id",
            how="inner",
        )

        # # group by category_name column and sum the order_item_quantity values
        grouped_df = (
            merged_df.groupby("category_name")["order_item_quantity"]
            .sum()
            .reset_index()
        )

        # # sort by order_count in descending order and keep the top 10 rows
        result_df = grouped_df.sort_values(
            by="order_item_quantity", ascending=False
        ).head(10)
        result_df.rename(columns={"order_item_quantity": "order_count"}, inplace=True)

        return (result_df, file_name)

    def orders_not_completed_by_state(self, orders_df, customers_df, file_name):
        # Merge the customers and orders dataframes on the customer_id column
        merged_df = orders_df.merge(
            customers_df, left_on="order_customer_id", right_on="customer_id"
        )

        # Filter the non-completed orders
        non_completed_orders = merged_df[
            ~merged_df["order_status"].isin(["COMPLETE", "CLOSED"])
        ]

        # Group the data by customer_state and count the number of non-completed orders
        state_counts_df = (
            non_completed_orders.groupby("customer_state")["order_id"]
            .count()
            .reset_index()
        )

        # Rename the 'order_id' column to 'non_completed_orders'
        state_counts_df = state_counts_df.rename(
            columns={"order_id": "non_completed_orders"}
        )

        # state_counts_df['non_completed_orders'] = state_counts_df['order_id']

        # Sort the dataframe by the number of non-completed orders in descending order
        state_counts_df = state_counts_df.sort_values(
            "non_completed_orders", ascending=False
        )

        return (state_counts_df, file_name)

    def dates_with_more_sales(self, orders_df, order_items_df, file_name):
        # Merge the Orders and Order Items dataframes on the order_id column
        merged_df = orders_df.merge(
            order_items_df, left_on="order_id", right_on="order_item_order_id"
        )

        # Calculate the total sales for each order
        merged_df["total_sales"] = (
            merged_df["order_item_quantity"] * merged_df["order_item_product_price"]
        )

        sales_by_date = (
            merged_df.groupby("order_date")["total_sales"].sum().reset_index()
        )

        # Rename the column containing the sum to "total_sales"
        sales_by_date = sales_by_date.rename(columns={"total_sales": "total_sales"})

        sales_by_date["order_date"] = pd.to_datetime(
            sales_by_date["order_date"]
        ).dt.date

        # Sort the results by the total sales in descending order
        sorted_sales = sales_by_date.sort_values(by="total_sales", ascending=False)

        return (sorted_sales, file_name)
