import express from "express"
import cors from "cors"
import { config } from "#@/config/config.js"
import helmet from "helmet"
import morgan from "morgan"
import rateLimit from "express-rate-limit"
import routes from "#@/routes.js"
import { errorHandler } from "#@/shared/middlewares/error.middleware.js"
import cookieParser from "cookie-parser"
import "dotenv/config"
import http from "http";
import { initSocket } from "./infrastructure/websocket/socket.manager.js";

const app = express()
app.set("trust proxy", 1);


app.use(helmet())
app.use(cors({
    origin: config.corsOrigins,
    credentials: true
}));
app.use(express.json())

app.use(cookieParser())
app.use(rateLimit(config.rateLimit))
app.use(express.urlencoded({ extended: true }))
app.use(morgan("dev"))

app.use("/api", routes);
app.get("/health", (_req, res) => {
    res.status(200).json({ message: "OK" })
})
app.use(errorHandler)
const server = http.createServer(app)
initSocket(server);

export { server };
export default app;