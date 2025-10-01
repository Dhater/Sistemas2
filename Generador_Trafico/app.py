import os
import pandas as pd
import numpy as np
import time
import redis
import psycopg2
from datetime import datetime

# 🔹 Ejecutar ingresar.py antes de nada
import ingresar
ingresar.main()  # ejecutar la función main() de ingresar.py

class TrafficGenerator:
    def __init__(self):
        self.df = pd.read_csv('/data/yahoo_answers.csv')
        self.redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'cache'), 
            port=6379, 
            decode_responses=True
        )
        self.db_conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'database'),
            database=os.getenv('DB_NAME', 'yahoo_qa'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'password123')
        )
        
    def poisson_distribution(self, lambda_param=1.0):
        return np.random.poisson(lambda_param)
    
    def uniform_distribution(self, min_time=0.1, max_time=2.0):
        return np.random.uniform(min_time, max_time)
    
    def get_random_question(self):
        return self.df.sample(n=1).iloc[0]
    
    def simulate_traffic(self):
        distribution = os.getenv('TRAFFIC_DISTRIBUTION', 'poisson')
        num_queries = int(os.getenv('NUM_QUERIES', 10000))
        
        for i in range(num_queries):
            wait_time = self.poisson_distribution() if distribution == 'poisson' else self.uniform_distribution()
            time.sleep(wait_time)
            
            question_data = self.get_random_question()
            question = question_data['Question']
            human_answer = question_data['Answer']
            
            print(f"Query {i}: {question[:100]}...")
            
        print("Simulación de tráfico completada")

if __name__ == "__main__":
    generator = TrafficGenerator()
    generator.simulate_traffic()
