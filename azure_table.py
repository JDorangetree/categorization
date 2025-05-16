import os
from azure.data.tables import TableClient, TableEntity, TableServiceClient # Importa TableTransactionType
from azure.core import exceptions
from typing import List, Dict, Any
import datetime
import pandas as pd
from dotenv import load_dotenv
from unidecode import unidecode
import pandas.api.types as ptypes

# --- Configuración (igual que antes) ---
load_dotenv()
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
table_name = "GPCsegments" # Reemplaza con el nombre de tu tabla

if not connection_string or table_name == "TuNombreDeTabla":
    print("Error: Configuración incompleta (cadena de conexión o nombre de tabla).")
    exit()

#--- Datos para una sola partición ---
#Nota: Todos estos documentos tienen la misma PartitionKey ('CategoriaA')
df_segments = pd.read_csv('segmentos.csv', sep=';', encoding='latin-1')
df_segments['RowKey'] = df_segments['RowKey'].astype('string')
# for columna in df_segments.columns:
#     # Verifica si la columna es de un tipo que contiene strings
#     # Usamos ptypes.is_string_dtype para una verificación más robusta
#     if ptypes.is_string_dtype(df_segments[columna]):
#         print(f"Aplicando unidecode a la columna: '{columna}'")
#         # Aplica la función unidecode a cada elemento de la columna
#         # .apply() es un método de Series que aplica una función a cada elemento
#         # Maneja automáticamente valores NaN (unidecode(NaN) puede variar, pero apply suele ser seguro)
#         df_segments[columna] = df_segments[columna].apply(unidecode)
#     else:
#         print(f"Saltando columna '{columna}' (no es de tipo string).")
records_list = df_segments.to_dict(orient='records')
#records_list = records_list[0:2]

# nueva_llave = "PartitionKey"
# nuevo_valor = 'Segmentos'
# llave_antigua = "segment_id"
# llave_nueva = "RowKey"

# Itera sobre la lista y agrega la llave a cada diccionario
# for diccionario in records_list:
#   diccionario[nueva_llave] = nuevo_valor
#   if llave_antigua in diccionario:
#     # Obtiene el valor asociado a la llave antigua y luego elimina esa llave
#     # .pop(llave) devuelve el valor y elimina la llave del diccionario
#     valor = diccionario.pop(llave_antigua)
#     diccionario[llave_nueva] = valor

print(records_list)

# my_entity = { 
#     u'PartitionKey' : 'Alemania' , 
#     u'RowKey' : 'Berlín' , 
#     u'Landmark' : 'Muro de Berlín' , 
#     u'Type' : 'Monumento histórico' ,     
#     u'Status' : True
#  } 
#--- Conectar a la Tabla ---
print(f"Conectando a la tabla '{table_name}' para transacción batch...")
table_service_client = TableServiceClient.from_connection_string(
        conn_str=connection_string
    )
table_client = table_service_client.get_table_client(table_name=table_name)
try:
    for entity in records_list:
        new_entity = table_client.upsert_entity(entity=entity)
        print ( f"Entidad de impresión creada {entity} " )
except Exception as e:
    print(f"Error al crear la entidad: {e}")
