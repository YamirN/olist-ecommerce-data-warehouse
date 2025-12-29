import pandas as pd
import csv

# Rutas
input_file = r'C:\Users\Yamir\Documents\DWH_Olist\dataset_olist\olist_order_reviews_dataset.csv'
output_file = r'C:\Users\Yamir\Documents\DWH_Olist\dataset_olist\olist_order_reviews_CLEAN.csv'

# Leer con Pandas (que maneja bien saltos de línea internos)
df = pd.read_csv(input_file, quotechar='"', encoding='utf-8')

# Reemplazar saltos de línea en columnas de texto
cols_texto = ['review_comment_title', 'review_comment_message']
for col in cols_texto:
    df[col] = df[col].astype(str).str.replace('\n', ' ').str.replace('\r', '')

# Guardar CSV limpio (usando | como separador para evitar líos con comas)
df.to_csv(output_file, sep='|', index=False, encoding='utf-8', quoting=csv.QUOTE_MINIMAL)

print("✅ Archivo limpio generado!")
