import pandas as pd
from google.cloud import storage


df = pd.read_csv('amazon.csv')


df.actual_price = df.actual_price.apply(lambda x: x.replace("₹","").replace(",",""))
df.actual_price = df.actual_price.astype("float") * 0.23
df.discounted_price = df.discounted_price.apply(lambda x: x.replace("₹","").replace(",",""))
df.discounted_price = df.discounted_price.astype("float") * 0.23


storage_client = storage.Client()


bucket = storage_client.create_bucket('amazonsalesdata')


table = bucket.create_table(
    'amazonsalesdata',
    ['product_id', 'product_name', 'category', 'discounted_price', 'actual_price',
    'discount_percentage', 'rating', 'rating_count', 'about_product', 'user_id', 'user_name',
    'review_id', 'review_title', 'review_content', 'img_link', 'product_link']
)

table.upload_data(df.to_dataframe())
