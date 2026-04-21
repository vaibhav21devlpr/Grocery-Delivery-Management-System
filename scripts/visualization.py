import pandas as pd
import matplotlib.pyplot as plt

# Load data
sales = pd.read_csv("data/output/sales_analysis.csv")
customers = pd.read_csv("data/output/customer_analysis.csv")
delivery = pd.read_csv("data/output/delivery_analysis.csv")


# 1. Top Products by Revenue

top_products = sales.sort_values(by="total_revenue", ascending=False).head(10)

plt.figure()
plt.bar(top_products["product_name"], top_products["total_revenue"])
plt.xticks(rotation=45)
plt.title("Top 10 Products by Revenue")
plt.xlabel("Product")
plt.ylabel("Revenue")
plt.tight_layout()
plt.savefig("data/output/top_products.png")
plt.show()


# 2. Customer Segmentation

customer_counts = customers["customer_tier"].value_counts()

plt.figure()
plt.pie(customer_counts, labels=customer_counts.index, autopct='%1.1f%%')
plt.title("Customer Segmentation")
plt.savefig("data/output/customer_segmentation.png")
plt.show()


# 3. Delivery Performance

top_delivery = delivery.sort_values(by="on_time_%", ascending=False).head(10)

plt.figure()
plt.bar(top_delivery["name"], top_delivery["on_time_%"])
plt.xticks(rotation=45)
plt.title("Top Delivery Personnel (On-Time %)")
plt.xlabel("Name")
plt.ylabel("On-Time %")
plt.tight_layout()
plt.savefig("data/output/delivery_performance.png")
plt.show()

print("✅ Visualizations created successfully!")