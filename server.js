require('dotenv').config();
const express = require('express');
const Redis = require('ioredis');

const app = express();
app.use(express.json());

// Conexão com o Redis local ou AWS Elasticache
const redisClient = new Redis(process.env.REDIS_URL || 'redis://127.0.0.1:6379');

redisClient.on('connect', () => console.log('🔌 Conectado ao Redis com sucesso.'));
redisClient.on('error', (err) => console.error(' Erro no Redis:', err));

// GATEWAY DE WEBHOOKS UNIFICADO
app.post('/v1/youshop-gateway', async (req, res) => {
    try {
        const { event_type, payload } = req.body;
        const authToken = req.headers['x-youshop-token'];

        // Validação de Segurança
        if (!authToken || authToken !== (process.env.YOUSHOP_AUTH_TOKEN || 'secret_dory_token')) {
            return res.status(401).json({ error: 'Token de autenticação inválido ou ausente.' });
        }

        if (!event_type || !payload) {
            return res.status(400).json({ error: 'Formato de payload inválido. Requer event_type e payload.' });
        }

        // Valida e direciona o evento para a fila correta no Redis
        const eventData = JSON.stringify({ event_type, payload, timestamp: Date.now() });
        
        if (event_type === 'product_traction_alert') {
            await redisClient.lpush('queue:ai_hub_campaigns', eventData);
        } else if (event_type === 'checkout_abandoned') {
            await redisClient.lpush('queue:checkout_recovery', eventData);
        } else {
            return res.status(400).json({ error: 'Tipo de evento não suportado por esta esteira de IA.' });
        }

        return res.status(202).json({ 
            status: 'Sucesso', 
            message: `Evento '${event_type}' enfileirado de forma assíncrona.` 
        });

    } catch (error) {
        console.error('Erro no Gateway Node.js:', error);
        return res.status(500).json({ error: 'Erro interno no gateway de microsserviços.' });
    }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`🐉 Dory Gateway [Node.js] ativado na porta ${PORT}`);
});