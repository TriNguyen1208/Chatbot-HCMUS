import app, { server } from "#@/app.js"
import {config} from "#@/config/config.js"
import { redisClient } from "#@/infrastructure/redis/redis.client.js"
import { mongoDB } from "#@/infrastructure/database/mongodb.connection.js"
import { initializeDatabaseModels } from "#@/infrastructure/database/init-models.js"
import "#@/background/workers/index.js"
import { queueService } from "#@/background/queue.service.js"
import { checkElasticsearchConnection } from "#@/infrastructure/elasticsearch/es.client.js"
import { initializeIndices } from "#@/infrastructure/elasticsearch/es.indices.js"

const start = async(): Promise<void> => {
    await Promise.all([
        mongoDB.connect().then(() => initializeDatabaseModels()),
        redisClient.connect(),
        checkElasticsearchConnection().then(() => initializeIndices()),
        queueService.initCronJobs()
    ])
    
    server.listen(config.port, () => {
        console.log(`Server is running on port ${config.port}`)
    })
}

await start()