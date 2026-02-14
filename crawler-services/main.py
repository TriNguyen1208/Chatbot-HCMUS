from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes import crawler_routes
import uvicorn

app = FastAPI(
    title="Crawler Services",
    description= "Crawler all the content of HCMUS"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(crawler_routes.router, prefix="/api")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=3001)