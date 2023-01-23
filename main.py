import pandas as pd
import pymysql


df = pd.read_csv('amazon.csv')


df.actual_price = df.actual_price.apply(lambda x: x.replace("₹","").replace(",",""))
df.actual_price = df.actual_price.astype("float") * 0.23
df.discounted_price = df.discounted_price.apply(lambda x: x.replace("₹","").replace(",",""))
df.discounted_price = df.discounted_price.astype("float") * 0.23


connection = pymysql.connect(user='root', password='123456', host='localhost', database='amazonsales')
cursor = connection.cursor()


cursor.execute("CREATE TABLE amazonsalesdata1 (product_id VARCHAR(500), product_name VARCHAR(500), category VARCHAR(500), discounted_price VARCHAR(500), actual_price VARCHAR(500), discount_percentage VARCHAR(500), rating VARCHAR(500), rating_count VARCHAR(500), about_product VARCHAR(100), user_id VARCHAR(500), user_name VARCHAR(500), review_id VARCHAR(500), review_title VARCHAR(500), review_content VARCHAR(500), img_link VARCHAR(500), product_link VARCHAR(500))")


for index, row in df.iterrows():
    cursor.execute("INSERT INTO amazonsalesdata1 (product_id, product_name, category, discounted_price, actual_price, discount_percentage, rating, rating_count, about_product, user_id, user_name, review_id, review_title, review_content, img_link, product_link) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (row['product_id'], row['product_name'], row['category'], row['discounted_price'], row['actual_price'], row['discount_percentage'], row['rating'], row['rating_count'], row['about_product'], row['user_id'], row['user_name'], row['review_id'], row['review_title'], row['review_content'], row['img_link'], row['product_link']))


connection.commit()


cursor.close()
connection.close()