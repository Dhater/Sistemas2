import redis
import os
import json
import time
from typing import Optional, Any, Dict, List
from dotenv import load_dotenv

load_dotenv()

class CacheManager:
    def __init__(self):
        self.redis_host = os.getenv('REDIS_HOST', 'localhost')
        self.redis_port = int(os.getenv('REDIS_PORT', 6379))
        self.cache_policy = os.getenv('CACHE_POLICY', 'LRU')
        self.cache_size = int(os.getenv('CACHE_SIZE', 100))
        self.cache_ttl = int(os.getenv('CACHE_TTL', 3600))
        
        # Conectar a Redis
        self.redis_client = redis.Redis(
            host=self.redis_host,
            port=self.redis_port,
            db=0,
            decode_responses=True
        )
        
        # Verificar conexión
        try:
            self.redis_client.ping()
            print(f"✅ Conectado a Redis en {self.redis_host}:{self.redis_port}")
            print(f"🔧 Política de caché: {self.cache_policy}")
            print(f"📊 Tamaño máximo: {self.cache_size} elementos")
            print(f"⏰ TTL: {self.cache_ttl} segundos")
        except redis.ConnectionError:
            print("❌ Error al conectar con Redis")
            raise
    
    def _get_cache_size(self) -> int:
        """Obtener el número actual de elementos en caché"""
        return self.redis_client.dbsize()
    
    def _evict_if_needed(self, key: str) -> None:
        """Aplicar política de evicción si el caché está lleno"""
        current_size = self._get_cache_size()
        
        if current_size >= self.cache_size:
            if self.cache_policy.upper() == 'LRU':
                self._evict_lru()
            elif self.cache_policy.upper() == 'FIFO':
                self._evict_fifo()
            elif self.cache_policy.upper() == 'LFU':
                self._evict_lfu()
            else:
                # Por defecto, eliminar aleatoriamente
                self._evict_random()
    
    def _evict_lru(self) -> None:
        """Eliminar el elemento menos recientemente usado"""
        # Redis maneja LRU automáticamente con maxmemory-policy
        # Para implementación manual, usaríamos sorted sets con timestamps
        keys = self.redis_client.keys('*')
        if keys:
            # Eliminar la clave más antigua (simplificado)
            self.redis_client.delete(keys[0])
    
    def _evict_fifo(self) -> None:
        """Eliminar el elemento más antiguo (First In, First Out)"""
        keys = self.redis_client.keys('*')
        if keys:
            # En una implementación real, usaríamos una lista para trackear orden
            # Esta es una simplificación
            oldest_key = min(keys, key=lambda k: self.redis_client.object('idletime', k))
            self.redis_client.delete(oldest_key)
    
    def _evict_lfu(self) -> None:
        """Eliminar el elemento menos frecuentemente usado"""
        keys = self.redis_client.keys('*')
        if keys:
            # Simplificación - en producción usaríamos HyperLogLog o contadores
            random_key = keys[0]
            self.redis_client.delete(random_key)
    
    def _evict_random(self) -> None:
        """Eliminar un elemento aleatorio"""
        keys = self.redis_client.keys('*')
        if keys:
            import random
            key_to_delete = random.choice(keys)
            self.redis_client.delete(key_to_delete)
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Almacenar un valor en caché
        
        Args:
            key: Clave del caché
            value: Valor a almacenar
            ttl: Tiempo de vida en segundos (opcional)
        
        Returns:
            bool: True si se almacenó correctamente
        """
        try:
            # Aplicar política de evicción si es necesario
            self._evict_if_needed(key)
            
            # Convertir valor a JSON si es necesario
            if isinstance(value, (dict, list)):
                value_to_store = json.dumps(value)
            else:
                value_to_store = str(value)
            
            # Establecer TTL
            actual_ttl = ttl if ttl is not None else self.cache_ttl
            
            result = self.redis_client.setex(
                key, 
                actual_ttl, 
                value_to_store
            )
            
            if result:
                print(f"✅ Almacenado en caché: {key} (TTL: {actual_ttl}s)")
            else:
                print(f"❌ Error al almacenar: {key}")
            
            return result
            
        except Exception as e:
            print(f"❌ Error en set cache: {e}")
            return False
    
    def get(self, key: str) -> Optional[Any]:
        """
        Obtener un valor del caché
        
        Args:
            key: Clave del caché
        
        Returns:
            Valor almacenado o None si no existe
        """
        try:
            value = self.redis_client.get(key)
            
            if value is None:
                print(f"🔍 Miss de caché: {key}")
                return None
            
            print(f"✅ Hit de caché: {key}")
            
            # Intentar decodificar JSON
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
                
        except Exception as e:
            print(f"❌ Error en get cache: {e}")
            return None
    
    def delete(self, key: str) -> bool:
        """Eliminar una clave del caché"""
        try:
            result = self.redis_client.delete(key)
            if result > 0:
                print(f"🗑️ Eliminado de caché: {key}")
                return True
            else:
                print(f"🔍 Clave no encontrada para eliminar: {key}")
                return False
        except Exception as e:
            print(f"❌ Error al eliminar clave: {e}")
            return False
    
    def exists(self, key: str) -> bool:
        """Verificar si una clave existe en caché"""
        try:
            return self.redis_client.exists(key) > 0
        except Exception as e:
            print(f"❌ Error al verificar existencia: {e}")
            return False
    
    def clear(self) -> bool:
        """Limpiar todo el caché"""
        try:
            result = self.redis_client.flushdb()
            print("🧹 Caché limpiado completamente")
            return result
        except Exception as e:
            print(f"❌ Error al limpiar caché: {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Obtener estadísticas del caché"""
        try:
            info = self.redis_client.info()
            stats = {
                'cache_policy': self.cache_policy,
                'max_size': self.cache_size,
                'default_ttl': self.cache_ttl,
                'current_size': self._get_cache_size(),
                'memory_used': info.get('used_memory_human', 'N/A'),
                'hits': info.get('keyspace_hits', 0),
                'misses': info.get('keyspace_misses', 0),
                'hit_rate': self._calculate_hit_rate(info)
            }
            return stats
        except Exception as e:
            print(f"❌ Error al obtener estadísticas: {e}")
            return {}
    
    def _calculate_hit_rate(self, info: Dict) -> float:
        """Calcular tasa de aciertos"""
        hits = info.get('keyspace_hits', 0)
        misses = info.get('keyspace_misses', 0)
        total = hits + misses
        return (hits / total * 100) if total > 0 else 0.0

def main():
    """Función principal para demostrar el uso del caché"""
    try:
        cache = CacheManager()
        
        # Ejemplo de uso
        print("\n🧪 Probando caché...")
        
        # Almacenar algunos valores
        cache.set("usuario:123", {"nombre": "Juan", "email": "juan@example.com"})
        cache.set("config:api_url", "https://api.ejemplo.com", ttl=1800)
        cache.set("contador", 42)
        
        # Recuperar valores
        usuario = cache.get("usuario:123")
        print(f"Usuario recuperado: {usuario}")
        
        # Verificar existencia
        if cache.exists("config:api_url"):
            print("✅ Configuración existe en caché")
        
        # Mostrar estadísticas
        stats = cache.get_stats()
        print(f"\n📊 Estadísticas del caché:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
        
        # Mantener el servicio corriendo
        print(f"\n🚀 Servicio de caché iniciado. Política: {cache.cache_policy}")
        print("Presiona Ctrl+C para detener...")
        
        while True:
            time.sleep(10)
            # Aquí podrías agregar monitoreo periódico o tareas de mantenimiento
            
    except KeyboardInterrupt:
        print("\n🛑 Servicio de caché detenido")
    except Exception as e:
        print(f"❌ Error en el servicio de caché: {e}")

if __name__ == "__main__":
    main()