import app, { server } from "#@/app.js"
import {config} from "#@/config/config.js"
import { redisClient } from "#@/infrastructure/redis/redis.js"
import { supabaseDB } from "#@/infrastructure/database/supabaseClient.js"
import { mongoDB } from "#@/infrastructure/database/mongoDBAtlas.js"
import "#@/modules/queue/queue.worker.js"
import { checkElasticsearchConnection } from "#@/infrastructure/elasticsearch/index.js"
import { initializeIndices } from "#@/infrastructure/elasticsearch/mapping.js"
import { rabbitmq } from "#@/infrastructure/rabbitmq/index.js"
import { startConsumers } from "#@/infrastructure/rabbitmq/consumer.js"

const start = async(): Promise<void> => {
    await Promise.all([
        mongoDB.connect(),
        supabaseDB.connect(),
        redisClient.connect(),
        checkElasticsearchConnection().then(() => initializeIndices()),
        rabbitmq.connect().then(() => startConsumers())
    ])
    
    server.listen(config.port, () => {
        console.log(`Server is running on port ${config.port}`)
    })
}

await start()