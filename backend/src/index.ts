import app from "#@/app.js"
import {config} from "#@/config/config.js"
import { redisClient } from "#@/infrastructure/redis/redis.js"
import { supabaseDB } from "#@/infrastructure/database/supabaseClient.js"
import { mongoDB } from "#@/infrastructure/database/mongoDBAtlas.js"

const start = async(): Promise<void> => {
    await Promise.all([
        mongoDB.connect(),
        supabaseDB.connect(),
        redisClient.connect()    
    ])
    
    app.listen(config.port, () => {
        console.log(`Server is running on port ${config.port}`)
    })
}

await start()

// (db) 
// findUser() => db.insert();