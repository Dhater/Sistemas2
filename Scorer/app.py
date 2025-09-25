import os
import psycopg2
import numpy as np
from sentence_transformers import SentenceTransformer, util
from transformers import pipeline
from sklearn.metrics.pairwise import cosine_similarity
import time
from datetime import datetime
from dotenv import load_dotenv
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

class Scorer:
    def __init__(self):
        # Configuración de la base de datos
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'database': os.getenv('DB_NAME', 'yahoo_finance'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', ''),
            'port': os.getenv('DB_PORT', '5432')
        }
        
        # Cargar modelos de evaluación
        logger.info("Cargando modelos de evaluación...")
        self.similarity_model = SentenceTransformer('all-MiniLM-L6-v2')
        self.quality_model = pipeline(
            "text-classification",
            model="distilbert-base-uncased-finetuned-sst-2-english",
            return_all_scores=True
        )
        
        # Conectar a la base de datos
        self.db_connection = self._connect_to_db()
        
        logger.info("✅ Scorer inicializado correctamente")
    
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
    
    def calculate_similarity(self, text1: str, text2: str) -> float:
        """
        Calcular similitud semántica entre dos textos usando cosine similarity
        
        Args:
            text1: Primer texto
            text2: Segundo texto
        
        Returns:
            float: Puntuación de similitud (0-1)
        """
        try:
            # Generar embeddings
            embedding1 = self.similarity_model.encode(text1, convert_to_tensor=True)
            embedding2 = self.similarity_model.encode(text2, convert_to_tensor=True)
            
            # Calcular similitud coseno
            similarity = util.pytorch_cos_sim(embedding1, embedding2)
            
            return float(similarity.item())
        except Exception as e:
            logger.error(f"Error calculando similitud: {e}")
            return 0.0
    
    def calculate_quality_score(self, text: str) -> float:
        """
        Calcular calidad del texto usando modelo de sentimiento/calidad
        
        Args:
            text: Texto a evaluar
        
        Returns:
            float: Puntuación de calidad (0-1)
        """
        try:
            # Si el texto es muy corto, retornar puntuación baja
            if len(text.split()) < 3:
                return 0.3
            
            # Usar modelo de clasificación para evaluar calidad
            results = self.quality_model(text[:512])  # Limitar longitud para eficiencia
            
            # Extraer puntuación positiva (como proxy de calidad)
            positive_score = 0.5  # Valor por defecto
            
            for result in results[0]:
                if result['label'] == 'POSITIVE':
                    positive_score = result['score']
                    break
            
            return positive_score
        except Exception as e:
            logger.error(f"Error calculando calidad: {e}")
            return 0.5
    
    def calculate_completeness(self, human_answer: str, llm_answer: str) -> float:
        """
        Calcular completitud de la respuesta comparada con la respuesta humana
        
        Args:
            human_answer: Respuesta de referencia
            llm_answer: Respuesta del LLM
        
        Returns:
            float: Puntuación de completitud (0-1)
        """
        try:
            human_words = set(human_answer.lower().split())
            llm_words = set(llm_answer.lower().split())
            
            if not human_words:
                return 0.0
            
            # Calcular cobertura de palabras clave
            common_words = human_words.intersection(llm_words)
            coverage = len(common_words) / len(human_words)
            
            return min(coverage, 1.0)
        except Exception as e:
            logger.error(f"Error calculando completitud: {e}")
            return 0.0
    
    def calculate_overall_score(self, similarity: float, quality: float, completeness: float) -> float:
        """
        Calcular puntuación general ponderada
        
        Args:
            similarity: Puntuación de similitud
            quality: Puntuación de calidad
            completeness: Puntuación de completitud
        
        Returns:
            float: Puntuación general (0-1)
        """
        # Pesos para cada métrica
        weights = {
            'similarity': 0.5,    # Más importancia a la similitud semántica
            'quality': 0.3,       # Calidad del texto
            'completeness': 0.2   # Completitud de la información
        }
        
        overall_score = (
            weights['similarity'] * similarity +
            weights['quality'] * quality +
            weights['completeness'] * completeness
        )
        
        return round(overall_score, 4)
    
    def get_pending_evaluations(self) -> list:
        """
        Obtener preguntas pendientes de evaluación
        
        Returns:
            list: Lista de tuplas con (id, question_text, human_answer, llm_answer)
        """
        try:
            cursor = self.db_connection.cursor()
            
            query = """
            SELECT id, question_text, human_answer, llm_answer 
            FROM questions 
            WHERE similarity_score IS NULL 
            AND llm_answer IS NOT NULL
            LIMIT 10
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            
            logger.info(f"📊 Encontradas {len(results)} evaluaciones pendientes")
            return results
            
        except Exception as e:
            logger.error(f"Error obteniendo evaluaciones pendientes: {e}")
            return []
    
    def update_evaluation(self, question_id: int, similarity_score: float, 
                         quality_score: float, completeness_score: float, 
                         overall_score: float) -> bool:
        """
        Actualizar la base de datos con los resultados de la evaluación
        
        Args:
            question_id: ID de la pregunta
            similarity_score: Puntuación de similitud
            quality_score: Puntuación de calidad
            completeness_score: Puntuación de completitud
            overall_score: Puntuación general
        
        Returns:
            bool: True si se actualizó correctamente
        """
        try:
            cursor = self.db_connection.cursor()
            
            query = """
            UPDATE questions 
            SET similarity_score = %s, 
                quality_score = %s,
                completeness_score = %s,
                overall_score = %s,
                evaluated_at = %s
            WHERE id = %s
            """
            
            cursor.execute(query, (
                similarity_score,
                quality_score,
                completeness_score,
                overall_score,
                datetime.now(),
                question_id
            ))
            
            self.db_connection.commit()
            cursor.close()
            
            logger.info(f"✅ Evaluación actualizada para pregunta ID: {question_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error actualizando evaluación: {e}")
            self.db_connection.rollback()
            return False
    
    def evaluate_single_question(self, question_data: tuple) -> bool:
        """
        Evaluar una sola pregunta
        
        Args:
            question_data: Tupla con (id, question_text, human_answer, llm_answer)
        
        Returns:
            bool: True si se evaluó correctamente
        """
        try:
            question_id, question_text, human_answer, llm_answer = question_data
            
            logger.info(f"🔍 Evaluando pregunta ID: {question_id}")
            
            # Calcular métricas individuales
            similarity = self.calculate_similarity(human_answer, llm_answer)
            quality = self.calculate_quality_score(llm_answer)
            completeness = self.calculate_completeness(human_answer, llm_answer)
            overall = self.calculate_overall_score(similarity, quality, completeness)
            
            # Actualizar base de datos
            success = self.update_evaluation(
                question_id, similarity, quality, completeness, overall
            )
            
            if success:
                logger.info(f"📈 Puntuaciones para ID {question_id}: "
                           f"Similitud: {similarity:.3f}, "
                           f"Calidad: {quality:.3f}, "
                           f"Completitud: {completeness:.3f}, "
                           f"General: {overall:.3f}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error evaluando pregunta ID {question_data[0]}: {e}")
            return False
    
    def run_evaluation_cycle(self):
        """Ejecutar un ciclo completo de evaluación"""
        try:
            # Obtener preguntas pendientes
            pending_questions = self.get_pending_evaluations()
            
            if not pending_questions:
                logger.info("⏳ No hay evaluaciones pendientes")
                return 0
            
            # Evaluar cada pregunta
            successful_evaluations = 0
            for question in pending_questions:
                if self.evaluate_single_question(question):
                    successful_evaluations += 1
            
            logger.info(f"🎯 Ciclo completado: {successful_evaluations}/{len(pending_questions)} evaluaciones exitosas")
            return successful_evaluations
            
        except Exception as e:
            logger.error(f"Error en ciclo de evaluación: {e}")
            return 0
    
    def get_evaluation_stats(self) -> dict:
        """Obtener estadísticas de evaluación"""
        try:
            cursor = self.db_connection.cursor()
            
            # Estadísticas generales
            query = """
            SELECT 
                COUNT(*) as total_questions,
                COUNT(overall_score) as evaluated_questions,
                AVG(overall_score) as avg_score,
                MIN(overall_score) as min_score,
                MAX(overall_score) as max_score
            FROM questions
            """
            
            cursor.execute(query)
            stats = cursor.fetchone()
            cursor.close()
            
            return {
                'total_questions': stats[0],
                'evaluated_questions': stats[1],
                'average_score': float(stats[2]) if stats[2] else 0,
                'min_score': float(stats[3]) if stats[3] else 0,
                'max_score': float(stats[4]) if stats[4] else 0
            }
            
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas: {e}")
            return {}
    
    def run_continuous_evaluation(self, interval_seconds: int = 30):
        """
        Ejecutar evaluación continua
        
        Args:
            interval_seconds: Intervalo entre ciclos de evaluación
        """
        logger.info(f"🚀 Iniciando evaluación continua (intervalo: {interval_seconds}s)")
        
        while True:
            try:
                # Ejecutar ciclo de evaluación
                self.run_evaluation_cycle()
                
                # Mostrar estadísticas periódicamente
                stats = self.get_evaluation_stats()
                if stats:
                    logger.info(f"📊 Estadísticas: {stats['evaluated_questions']}/{stats['total_questions']} "
                               f"evaluadas - Puntuación promedio: {stats.get('average_score', 0):.3f}")
                
                # Esperar antes del siguiente ciclo
                time.sleep(interval_seconds)
                
            except KeyboardInterrupt:
                logger.info("🛑 Evaluación detenida por el usuario")
                break
            except Exception as e:
                logger.error(f"Error en evaluación continua: {e}")
                time.sleep(interval_seconds)  # Continuar a pesar del error

def main():
    """Función principal"""
    try:
        # Inicializar scorer
        scorer = Scorer()
        
        # Mostrar estadísticas iniciales
        stats = scorer.get_evaluation_stats()
        logger.info(f"📈 Estadísticas iniciales: {stats}")
        
        # Iniciar evaluación continua
        scorer.run_continuous_evaluation(interval_seconds=30)
        
    except Exception as e:
        logger.error(f"❌ Error fatal en el Scorer: {e}")
        raise

if __name__ == "__main__":
    main()