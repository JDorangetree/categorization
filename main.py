from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
import google.generativeai as genai
import os
from google.api_core import exceptions
from typing import Optional
import json # Importa la librería json
from dotenv import load_dotenv
from unidecode import unidecode
import pandas as pd
from azure.data.tables import TableClient

# --- Configuración de Paths ---
# Obtiene la ruta del directorio donde está main.py
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Construye la ruta completa al archivo de prompts
PROMPTS_FILE_PATH = os.path.join(BASE_DIR, 'data', 'prompts.json')


# Carga las variables de entorno desde un archivo .env
load_dotenv()
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

# --- Cargar los prompts desde el archivo JSON al inicio ---
LOADED_PROMPTS: dict = {}
try:
    if os.path.exists(PROMPTS_FILE_PATH):
        with open(PROMPTS_FILE_PATH, 'r', encoding='utf-8') as f: # Usa encoding='utf-8'
            LOADED_PROMPTS = json.load(f)
        print(f"Prompts cargados desde {PROMPTS_FILE_PATH}")
    else:
        print(f"ADVERTENCIA: El archivo de prompts no se encontró en {PROMPTS_FILE_PATH}. Algunos endpoints podrían no funcionar correctamente.")
except json.JSONDecodeError:
    print(f"ERROR: El archivo {PROMPTS_FILE_PATH} no es un JSON válido.")
    # Puedes decidir salir o usar un diccionario vacío
    LOADED_PROMPTS = {}
except Exception as e:
    print(f"ERROR inesperado al cargar prompts desde {PROMPTS_FILE_PATH}: {e}")
    LOADED_PROMPTS = {}


# --- Configurar genai globalmente con la clave (si existe) ---
if GOOGLE_API_KEY:
     genai.configure(api_key=GOOGLE_API_KEY)
     print("API Key de Gemini cargada desde el entorno.")
else:
     print("ADVERTENCIA: GOOGLE_API_KEY no configurada. El endpoint /generate/ no funcionará.")

connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
if not connection_string:
    print("Error: La variable de entorno 'AZURE_STORAGE_CONNECTION_STRING' no está configurada.")
    print("Por favor, configúrala antes de ejecutar el script.")
    exit()


# Define el modelo de datos para la solicitud entrante de Gemini (AHORA SIN LA CLAVE)
class GeminiRequest(BaseModel):
    # Puedes agregar un campo opcional para especificar qué prompt usar del archivo
    prompt_key: Optional[str] = None # Clave para seleccionar un prompt del archivo
    user_description_input: str # La descripción del producto introducida para resumir


# Crea la instancia de la aplicación FastAPI
app = FastAPI(
    title="categorización" \
    " con Gemini" ,
    description="API para categorizar productos de consumo masivo bajo el arbol de categorías GPC de GS1 usando Gemini.",
    version="0.1.0",
)

# --- Endpoint para Gemini (USA LA CLAVE GLOBAL Y PROMPTS CARGADOS) ---
@app.post("/summarize-description/")
async def generate_description_with_gemini(request: GeminiRequest):
    """
    Recibe entrada del usuario (descripción corta de un producto ej: Leche ALQUERIA uat semiscremada original familiar x1LTR)
     llama a la API de Gemini usando la clave del entorno y el prompt cargado.
    """
    # Verifica si la clave de la API está configurada
    if GOOGLE_API_KEY is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="La API Key de Gemini no está configurada en el servidor."
        )

    # Verifica si la entrada del usuario está vacía
    if not request.user_description_input:
         raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="La 'user_description_input' no puede estar vacía."
        )

    # --- Construye el prompt completo para Gemini ---
    final_prompt_text = ""
    # Si el cliente especificó una clave de prompt
    instruction_template = LOADED_PROMPTS.get('generate_product_description_prompt')    
    final_prompt_text = instruction_template.format(
        user_input=request.user_description_input
    )

    # Asegúrate de que el prompt final no esté vacío
    if not final_prompt_text.strip():
         raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="El prompt final construido está vacío."
        )

    try:
        # Usa la clave configurada globalmente
        model = genai.GenerativeModel('gemini-2.5-flash-preview-04-17') # Revisa el nombre del modelo si usas otro

        #print(f"Enviando prompt a Gemini:\n---\n{final_prompt_text}\n---") # Log para depuración
        response = model.generate_content(final_prompt_text)

        # Verifica si la respuesta tiene texto antes de acceder
        if response.text is None:
             raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="La API de Gemini no devolvió texto en la respuesta."
            )


        return {"generated_description": response.text}

    except exceptions.GoogleAPIError as e:
        print(f"Error de API de Google: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error al interactuar con la API de Gemini: {e}"
        )
    except Exception as e:
        print(f"Error inesperado: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ocurrió un error interno: {e}"
        )

# --- NUEVO Endpoint: Generar y luego Interpretar/Procesar ---
@app.post("/generate-and-segment/")
async def generate_and_segmented_description(request: GeminiRequest):
    """
    Recibe descripción resumida, llama internamente a /generate/ para obtener texto de Gemini,
    y luego categoriza a nivel de segmento esa descripción.
    """
    # Llama a la función que maneja el endpoint /generate/.
    # Esto ejecuta la lógica de llamar a Gemini.
    # Captura cualquier HTTPException que pueda lanzar generate_text_with_gemini
    instruction_template = LOADED_PROMPTS.get('gpc_categorization_segment')
    final_prompt_text = instruction_template
        
    # Filtra por PartitionKey igual a 'CategoriaA'
    # La sintaxis del filtro usa OData
    table_name = "GPCsegments"
    try:
        table_client = TableClient.from_connection_string(
        conn_str=connection_string,
        table_name=table_name
    )
        try:
            list_of_entities = []
            all_entities = table_client.query_entities(query_filter=None)
            print(f"Entidades encontradas:")
            count = 0
            for entity in all_entities:
                list_of_entities.append(entity)
                #print(entity)
                count += 1
            print(f"Total de entidades encontradas: {count}")

        except Exception as e:
            print(f"Ocurrió un error al consultar por PartitionKey: {e}")
    except exceptions.ClientAuthenticationError as e:
        print(f"Error de autenticación: Verifica tu cadena de conexión. {e}")
    except exceptions.HttpResponseError as e:
        print(f"Error HTTP al conectar o validar la tabla: Status {e.status_code}, Mensaje: {e.message}")
    except Exception as e:
        print(f"Ocurrió un error inesperado al conectar o inicializar: {e}")



    df_segmentos = pd.DataFrame(list_of_entities)
    #df_segmentos = pd.read_csv('segmentos.csv', sep=';', encoding='latin-1')
    #df_segmentos['Segmento'] = df_segmentos['Segmento'].apply(unidecode)
    # Asegúrate de que el archivo CSV tiene una columna llamada 'segmento'
    # Define la lista de segmentos GPC
    segmentos = df_segmentos['Segmento'].tolist()


    # Asegúrate de que el prompt final no esté vacío
    if not final_prompt_text.strip():
         raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="El prompt final construido está vacío."
        )
    
    
    try:
        gemini_response_dict = await generate_description_with_gemini(request)
        generated_text = gemini_response_dict.get("generated_description", "")
        model = genai.GenerativeModel('gemini-2.5-flash-preview-04-17')

        #print(f"Enviando prompt a Gemini:\n---\n{final_prompt_text}\n---") # Log para depuración
        response = model.generate_content(final_prompt_text.format(
            "\n- ".join(segmentos),
            "\n- ".join(generated_text))
        )

        # --- Lógica de Interpretación/Procesamiento ---
        # Aquí es donde pondrías tu código para interpretar 'generated_text'
        
        interpreted_text = f"[PROCESADO] {generated_text} [FIN PROCESAMIENTO]" # Ejemplo simple

        # Puedes agregar más lógica aquí, como:
        # - Analizar sentimiento
        # - Extraer entidades clave
        # - Resumir (si el output de Gemini no era un resumen)
        # - Formatear el texto de otra manera

        # --- Fin Lógica de Interpretación/Procesamiento ---

        # Devuelve tanto el texto original generado como el interpretado
        return {"assigned_segment": response.text}
        # return {
        #     "original_generated_text": generated_text,
        #     "interpreted_text": interpreted_text,
        #     "message": "Texto generado por Gemini y luego procesado."
        # }

    except HTTPException as e:
        # Si la llamada interna a generate_text_with_gemini lanzó un HTTPException,
        # lo relanzamos para que FastAPI lo maneje y devuelva el error al cliente.
        raise e
    except Exception as e:
        # Captura cualquier otro error que pudiera ocurrir *después* de la llamada a generate_text_with_gemini,
        # o si la llamada interna falló de forma inesperada sin un HTTPException
        print(f"Error durante la interpretación o llamada interna: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error al procesar la respuesta de Gemini: {e}"
        )




# Endpoint raíz
@app.get("/")
async def read_root():
    status_msg = "configurada" if GOOGLE_API_KEY else "NO configurada"
    prompts_status = f"{len(LOADED_PROMPTS)} cargados" if LOADED_PROMPTS else "NO cargados o error"
    return {
        "message": "API lista.",
        "gemini_key_status": status_msg,
        "prompts_status": prompts_status,
        "endpoints": {
            "/generate/ (POST)": "Requiere user_input y opcionalmente prompt_key. Usa la clave del entorno y prompts cargados."
        }
    }