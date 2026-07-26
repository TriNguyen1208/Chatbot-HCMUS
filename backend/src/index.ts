import app from "#@/app.js"
import {config} from "#@/shared/config/config.js"
import { mongoDB } from "#@/shared/database/mongoDB.js"
import { redisClient } from "#@/shared/database/redis.js"

const start = async(): Promise<void> => {
    await mongoDB.connect()
    await redisClient.connect()    
    app.listen(config.port, () => {
        console.log(`Server is running on port ${config.port}`)
    })
}

await start()

// (db) 
// findUser() => db.insert();