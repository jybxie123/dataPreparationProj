import pandas as pd

# 加载CSV文件
df = pd.read_csv('/Users/jiangyanbo/working/course/dataPreparation/project/Food_Inspections_20240215.csv')

temp_df = df.head(100)
temp_df.to_csv('part_food.csv', index=False)

