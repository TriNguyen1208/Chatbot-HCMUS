import app from "#@/app.js"
import {config} from "#@/config/index.js"
import { database } from "./database/index.js"
import { redisClient } from "./database/redis.js"

const start = async(): Promise<void> => {
    await database.connect()
    await redisClient.connect()
    app.listen(config.port, () => {
        console.log(`Server is running on port ${config.port}`)
    })
}

await start()

