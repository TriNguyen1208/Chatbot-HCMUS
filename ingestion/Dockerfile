# Text Embeddings Inference (TEI) Dockerfile
# Base image supports: cpu-1.9, 86-1.9 (GPU Ampere), cpu-arm64-1.9 (Apple Silicon)
ARG TEI_VERSION=cpu-1.9
FROM ghcr.io/huggingface/text-embeddings-inference:${TEI_VERSION}

# Internal port exposed by TEI router
EXPOSE 80

# Default entrypoint launching intfloat/multilingual-e5-small model
ENTRYPOINT ["text-embeddings-router", "--model-id", "intfloat/multilingual-e5-small", "--port", "80"]
