import express from "express"
import cors from "cors"
import {config} from "#@/config/index.js"
import helmet from "helmet"
import morgan from "morgan"
import rateLimit from "express-rate-limit"
import routes from "#@/routes/index.js"
import { errorHandler } from "./middleware/error.middleware.js"

const app = express()

app.use(helmet())
app.use(cors())
app.use(express.json())

app.use(rateLimit(config.rateLimit))
app.use(express.urlencoded({extended: true}))
app.use(morgan("dev"))

app.use("/api/v1", routes);
app.get("/health", (_req, res) => {
    res.status(200).json({message: "OK"})
})
app.use(errorHandler)


export default app