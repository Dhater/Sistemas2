import redis
import os
import json
import time
import random
from typing import Optional, Any, Dict
from dotenv import load_dotenv

load_dotenv()

class CacheManager:
    def __init__(self, max_keys: int = 500):
        self.redis_host = os.getenv('REDIS_HOST', 'localhost')
        self.redis_port = int(os.getenv('REDIS_PORT', 6379))
        self.cache_policy = os.getenv('CACHE_POLICY', 'LRU')
        self.cache_size = max_keys  # Máximo de llaves permitido
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
        return self.redis_client.dbsize()

    def _evict_if_needed(self):
        """Eliminar elementos según la política si el caché excede el tamaño"""
        while self._get_cache_size() > self.cache_size:
            if self.cache_policy.upper() == 'LRU':
                self._evict_lru()
            elif self.cache_policy.upper() == 'FIFO':
                self._evict_fifo()
            elif self.cache_policy.upper() == 'LFU':
                self._evict_lfu()
            else:
                self._evict_random()

    def _evict_lru(self):
        keys = self.redis_client.keys('*')
        if keys:
            # Simplificado: eliminar la primera clave
            self.redis_client.delete(keys[0])
            print(f"🗑️ Evicción LRU: {keys[0]}")

    def _evict_fifo(self):
        keys = self.redis_client.keys('*')
        if keys:
            oldest_key = min(keys, key=lambda k: self.redis_client.object('idletime', k))
            self.redis_client.delete(oldest_key)
            print(f"🗑️ Evicción FIFO: {oldest_key}")

    def _evict_lfu(self):
        keys = self.redis_client.keys('*')
        if keys:
            # Simplificación: eliminar primera clave
            self.redis_client.delete(keys[0])
            print(f"🗑️ Evicción LFU: {keys[0]}")

    def _evict_random(self):
        keys = self.redis_client.keys('*')
        if keys:
            key_to_delete = random.choice(keys)
            self.redis_client.delete(key_to_delete)
            print(f"🗑️ Evicción Random: {key_to_delete}")

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        try:
            self._evict_if_needed()
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
            ttl = ttl if ttl else self.cache_ttl
            result = self.redis_client.setex(key, ttl, value)
            if result:
                print(f"✅ Almacenado: {key} (TTL={ttl}s)")
            return result
        except Exception as e:
            print(f"❌ Error al almacenar {key}: {e}")
            return False

    def get(self, key: str) -> Optional[Any]:
        try:
            value = self.redis_client.get(key)
            if value is None:
                print(f"🔍 Miss de caché: {key}")
                return None
            print(f"✅ Hit de caché: {key}")
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        except Exception as e:
            print(f"❌ Error al obtener {key}: {e}")
            return None

    def clear(self):
        self.redis_client.flushdb()
        print("🧹 Caché limpio")

    def get_stats(self) -> Dict[str, Any]:
        info = self.redis_client.info()
        return {
            'current_size': self._get_cache_size(),
            'memory_used': info.get('used_memory_human', 'N/A'),
            'hits': info.get('keyspace_hits', 0),
            'misses': info.get('keyspace_misses', 0)
        }

def main():
    cache = CacheManager(max_keys=500)

    # Ejemplo inicial
    cache.set("usuario:123", {"nombre": "Juan"})
    cache.set("contador", 42)

    print("\n🚀 Cache manager corriendo... Ctrl+C para detener.")

    try:
        while True:
            time.sleep(10)
            cache._evict_if_needed()  # Revisa y elimina si hace falta
            stats = cache.get_stats()
            print(f"📊 Tamaño: {stats['current_size']}, Hits: {stats['hits']}, Misses: {stats['misses']}")
    except KeyboardInterrupt:
        print("\n🛑 Cache manager detenido")

if __name__ == "__main__":
    main()