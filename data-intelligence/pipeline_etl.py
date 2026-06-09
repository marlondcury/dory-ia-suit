import os
import json
import time
import pandas as pd
import requests
from redis import Redis

# Configurações de ambiente
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379")
N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL", "http://localhost:5678/webhook/dory-suite-processor")

try:
    redis_client = Redis.from_url(REDIS_URL)
    print("🔌 Conexão do Motor Python com o Redis estabelecida.")
except Exception as e:
    print(f" Erro ao conectar o Python ao Redis: {e}")
    exit(1)

def process_ai_hub_campaigns(raw_data):
    """Tratamento para o Módulo 1: Campanhas de Atração para Creators"""
    event = json.loads(raw_data)
    df = pd.DataFrame([event['payload']])
    
    # Cálculo estatístico de tração (Métricas YouShop)
    df['traction_score'] = (df['conversion_rate'] * 70) + (df['total_clicks'] * 0.3)
    
    data_out = df.iloc[0].to_dict()
    data_out['module'] = 'ai_hub'
    return data_out

def process_checkout_recovery(raw_data):
    """Tratamento para o Módulo 3: Recuperação de Carrinho"""
    event = json.loads(raw_data)
    df = pd.DataFrame([event['payload']])
    
    # 🔒 LGPD COMPLIANCE: Anonimização total de dados estritamente sensíveis do comprador
    # Armazenamos apenas o primeiro nome e limpamos documentos de identificação fiscal
    if 'buyer_cpf' in df.columns:
        df = df.drop(columns=['buyer_cpf'])
    
    if 'client_name' in df.columns:
        df['client_name'] = df['client_name'].apply(lambda name: name.split()[0] if isinstance(name, str) else "Cliente")

    row = df.iloc[0].to_dict()
    
    # Inteligência de Negócio Conclusiva inferida pelo Pandas
    # Se parou no frete e o frete custa mais que 30% do produto, define a causa raiz do abandono
    if row.get('abandon_step') == 'shipping_selection' and (row.get('shipping_value', 0) / row.get('price', 1)) > 0.3:
        row['probable_drop_reason'] = 'frete_alto_regiao'
    else:
        row['probable_drop_reason'] = 'duvida_suporte_preco'
        
    row['module'] = 'checkout_copilot'
    return row

def main_loop():
    print("🐉 Motor Python de Inteligência e ETL iniciado. Monitorando filas...")
    
    while True:
        # Escuta ativa e síncrona em multiplas filas (Blpop)
        queue_triggered, raw_data = redis_client.brpop(['queue:ai_hub_campaigns', 'queue:checkout_recovery'])
        queue_name = queue_triggered.decode('utf-8')
        
        print(f"📦 Evento detectado na fila: {queue_name}")
        
        try:
            if queue_name == 'queue:ai_hub_campaigns':
                processed_payload = process_ai_hub_campaigns(raw_data)
            elif queue_name == 'queue:checkout_recovery':
                processed_payload = process_checkout_recovery(raw_data)
                
            # Dispara o lote de dados estruturado e higienizado diretamente para a IA no n8n
            response = requests.post(N8N_WEBHOOK_URL, json=processed_payload, timeout=10)
            print(f"✅ Contexto enviado ao n8n. Status do Orquestrador: {response.status_code}")
            
        except Exception as e:
            print(f" Falha crítica no processamento do evento do lote: {e}")
            
        time.sleep(0.5)

if __name__ == "__main__":
    main_loop()