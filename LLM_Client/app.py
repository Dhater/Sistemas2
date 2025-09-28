import os
import time
import google.generativeai as genai
import psycopg2
import redis
import json
import logging
from datetime import datetime
from dotenv import load_dotenv

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

class LLMClient:
    def __init__(self):
        # Configuración de Gemini
        self.gemini_api_key = os.getenv('GEMINI_API_KEY')
        if not self.gemini_api_key:
            raise ValueError("GEMINI_API_KEY no encontrada en variables de entorno")
        
        genai.configure(api_key=self.gemini_api_key)
        self.model = genai.GenerativeModel('gemini-pro')
        
        # Configuración de Redis
        self.redis_host = os.getenv('REDIS_HOST', 'cache')
        self.redis_port = int(os.getenv('REDIS_PORT', 6379))
        self.redis_client = redis.Redis(
            host=self.redis_host, 
            port=self.redis_port, 
            decode_responses=True
        )
        
        # Configuración de la base de datos
        self.db_config = {
            'host': os.getenv('DB_HOST', 'database'),
            'database': os.getenv('DB_NAME', 'yahoo_qa'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'password123'),
            'port': os.getenv('DB_PORT', '5432')
        }
        
        # Conectar a la base de datos
        self.db_connection = self._connect_to_db()
        
        logger.info("✅ LLM Client inicializado correctamente")
        logger.info(f"🔮 Modelo Gemini: gemini-pro")
    
    def _connect_to_db(self):
        """Conectar a la base de datos PostgreSQL"""
        max_retries = 5
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                conn = psycopg2.connect(**self.db_config)
                logger.info("✅ Conectado a la base de datos")
                return conn
            except Exception as e:
                logger.warning(f"❌ Intento {attempt + 1}/{max_retries} - Error de conexión: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise Exception(f"No se pudo conectar a la base de datos después de {max_retries} intentos")
    
    def _generate_cache_key(self, question: str) -> str:
        """Generar clave única para el caché basada en la pregunta"""
        # Normalizar la pregunta para evitar duplicados por espacios/capitalización
        normalized_question = question.strip().lower()
        return f"llm_answer:{hash(normalized_question)}"
    
    def get_answer_from_gemini(self, question: str) -> str:
        """
        Obtener respuesta de Gemini API para una pregunta
        
        Args:
            question: Pregunta a responder
        
        Returns:
            str: Respuesta del LLM
        """
        try:
            logger.info(f"🔮 Consultando Gemini API: {question[:50]}...")
            
            # Configurar el prompt para respuestas financieras
            prompt = f"""
            Eres un experto en finanzas y mercados bursátiles. Responde la siguiente pregunta de manera concisa y precisa.
            
            Pregunta: {question}
            
            Respuesta:
            """
            
            response = self.model.generate_content(prompt)
            
            if response and response.text:
                logger.info("✅ Respuesta obtenida de Gemini API")
                return response.text.strip()
            else:
                logger.error("❌ Respuesta vacía de Gemini API")
                return "No pude generar una respuesta para esta pregunta."
                
        except Exception as e:
            logger.error(f"❌ Error consultando Gemini API: {e}")
            return f"Error al obtener respuesta: {str(e)}"
    
    def get_cached_answer(self, question: str) -> str:
        """
        Buscar respuesta en caché
        
        Args:
            question: Pregunta a buscar
        
        Returns:
            str: Respuesta del caché o None si no existe
        """
        try:
            cache_key = self._generate_cache_key(question)
            cached_answer = self.redis_client.get(cache_key)
            
            if cached_answer:
                logger.info(f"✅ Respuesta encontrada en caché: {question[:30]}...")
                return cached_answer
            else:
                logger.info(f"🔍 Miss de caché: {question[:30]}...")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error accediendo al caché: {e}")
            return None
    
    def cache_answer(self, question: str, answer: str, ttl: int = 3600) -> bool:
        """
        Almacenar respuesta en caché
        
        Args:
            question: Pregunta
            answer: Respuesta
            ttl: Tiempo de vida en segundos
        
        Returns:
            bool: True si se almacenó correctamente
        """
        try:
            cache_key = self._generate_cache_key(question)
            success = self.redis_client.setex(cache_key, ttl, answer)
            
            if success:
                logger.info(f"💾 Respuesta almacenada en caché: {question[:30]}...")
            else:
                logger.error(f"❌ Error almacenando en caché: {question[:30]}...")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Error almacenando en caché: {e}")
            return False
    
    def get_answer_from_db(self, question: str) -> str:
        """
        Buscar respuesta en la base de datos
        
        Args:
            question: Pregunta a buscar
        
        Returns:
            str: Respuesta de la base de datos o None si no existe
        """
        try:
            cursor = self.db_connection.cursor()
            
            # Buscar pregunta similar (usando LIKE para similitud básica)
            query = """
            SELECT llm_answer FROM questions 
            WHERE question_text ILIKE %s 
            AND llm_answer IS NOT NULL
            LIMIT 1
            """
            
            cursor.execute(query, (f'%{question}%',))
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                logger.info(f"✅ Respuesta encontrada en BD: {question[:30]}...")
                return result[0]
            else:
                logger.info(f"🔍 Pregunta no encontrada en BD: {question[:30]}...")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error buscando en base de datos: {e}")
            return None
    
    def save_question_answer_to_db(self, question: str, llm_answer: str, human_answer: str = None) -> bool:
        """
        Guardar pregunta y respuesta en la base de datos
        
        Args:
            question: Pregunta
            llm_answer: Respuesta del LLM
            human_answer: Respuesta humana (opcional)
        
        Returns:
            bool: True si se guardó correctamente
        """
        try:
            cursor = self.db_connection.cursor()
            
            # Verificar si la pregunta ya existe
            check_query = "SELECT id FROM questions WHERE question_text = %s"
            cursor.execute(check_query, (question,))
            existing = cursor.fetchone()
            
            if existing:
                # Actualizar respuesta existente
                update_query = """
                UPDATE questions 
                SET llm_answer = %s, human_answer = COALESCE(%s, human_answer)
                WHERE question_text = %s
                """
                cursor.execute(update_query, (llm_answer, human_answer, question))
                logger.info(f"📝 Respuesta actualizada en BD: {question[:30]}...")
            else:
                # Insertar nueva pregunta
                insert_query = """
                INSERT INTO questions (question_text, human_answer, llm_answer) 
                VALUES (%s, %s, %s)
                """
                cursor.execute(insert_query, (question, human_answer, llm_answer))
                logger.info(f"📝 Nueva pregunta guardada en BD: {question[:30]}...")
            
            self.db_connection.commit()
            cursor.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ Error guardando en base de datos: {e}")
            self.db_connection.rollback()
            return False
    
    def process_question(self, question: str, use_cache: bool = True, save_to_db: bool = True) -> dict:
        """
        Procesar una pregunta completa: caché → BD → Gemini API
        
        Args:
            question: Pregunta a procesar
            use_cache: Usar caché (True/False)
            save_to_db: Guardar en base de datos (True/False)
        
        Returns:
            dict: Resultado con respuesta y metadatos
        """
        start_time = time.time()
        source = "unknown"
        
        try:
            # Paso 1: Buscar en caché
            if use_cache:
                cached_answer = self.get_cached_answer(question)
                if cached_answer:
                    response_time = time.time() - start_time
                    return {
                        'question': question,
                        'answer': cached_answer,
                        'source': 'cache',
                        'response_time': round(response_time, 3),
                        'timestamp': datetime.now().isoformat()
                    }
            
            # Paso 2: Buscar en base de datos
            db_answer = self.get_answer_from_db(question)
            if db_answer:
                # Almacenar en caché para futuras consultas
                if use_cache:
                    self.cache_answer(question, db_answer)
                
                response_time = time.time() - start_time
                source = "database"
                answer = db_answer
            else:
                # Paso 3: Consultar Gemini API
                gemini_answer = self.get_answer_from_gemini(question)
                source = "gemini"
                answer = gemini_answer
                
                # Guardar en base de datos y caché
                if save_to_db:
                    self.save_question_answer_to_db(question, gemini_answer)
                
                if use_cache:
                    self.cache_answer(question, gemini_answer)
            
            response_time = time.time() - start_time
            
            result = {
                'question': question,
                'answer': answer,
                'source': source,
                'response_time': round(response_time, 3),
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"✅ Pregunta procesada - Fuente: {source}, Tiempo: {response_time:.3f}s")
            return result
            
        except Exception as e:
            logger.error(f"❌ Error procesando pregunta: {e}")
            return {
                'question': question,
                'answer': f"Error: {str(e)}",
                'source': 'error',
                'response_time': round(time.time() - start_time, 3),
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }
    
    def get_stats(self) -> dict:
        """Obtener estadísticas del servicio"""
        try:
            cursor = self.db_connection.cursor()
            
            # Estadísticas de la base de datos
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_questions,
                    COUNT(llm_answer) as answered_questions,
                    COUNT(DISTINCT question_text) as unique_questions
                FROM questions
            """)
            db_stats = cursor.fetchone()
            
            # Estadísticas de Redis
            cache_stats = {
                'cache_size': self.redis_client.dbsize(),
                'connected_clients': self.redis_client.info('clients').get('connected_clients', 0)
            }
            
            cursor.close()
            
            return {
                'database': {
                    'total_questions': db_stats[0],
                    'answered_questions': db_stats[1],
                    'unique_questions': db_stats[2]
                },
                'cache': cache_stats,
                'service': {
                    'status': 'healthy',
                    'gemini_configured': bool(self.gemini_api_key)
                }
            }
            
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas: {e}")
            return {'error': str(e)}

def main():
    """Función principal para pruebas"""
    try:
        llm_client = LLMClient()
        
        # Ejemplo de uso
        test_questions = [
            "¿Qué es el mercado de valores?",
            "¿Cómo funciona el NASDAQ?",
            "¿Qué son los dividendos?"
        ]
        
        print("🧪 Probando LLM Client...")
        
        for question in test_questions:
            result = llm_client.process_question(question)
            print(f"\n❓ Pregunta: {result['question']}")
            print(f"✅ Respuesta: {result['answer'][:100]}...")
            print(f"📊 Fuente: {result['source']}")
            print(f"⏱️  Tiempo: {result['response_time']}s")
        
        # Mostrar estadísticas
        stats = llm_client.get_stats()
        print(f"\n📈 Estadísticas: {stats}")
        
        # Mantener el servicio corriendo para recibir consultas
        print("\n🚀 LLM Client iniciado. Esperando consultas...")
        
        # Aquí podrías agregar un servidor HTTP o consumir de una cola
        while True:
            time.sleep(10)
            
    except KeyboardInterrupt:
        print("\n🛑 LLM Client detenido")
    except Exception as e:
        print(f"❌ Error en LLM Client: {e}")
        raise

if __name__ == "__main__":
    main()