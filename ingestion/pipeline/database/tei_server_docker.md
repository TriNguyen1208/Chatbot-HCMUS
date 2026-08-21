# TEI Server with Docker

Run Hugging Face **Text Embeddings Inference (TEI)** with:

```text
Model: intfloat/multilingual-e5-small
Port: 8080
API: http://localhost:8080
```

`intfloat/multilingual-e5-small` is a public model, so **no Hugging Face token is required**.

**Note:** Before running, make sure your disk storage (eg: C:/) has at least 10 GB remaining.

---

## 1. Windows / Linux + NVIDIA GPU

### Check GPU

```bash
nvidia-smi
```

Make sure Docker can access the GPU:

```bash
docker run --rm --gpus all nvidia/cuda:12.9.1-base-ubuntu22.04 nvidia-smi
```

### Run TEI

For **Ampere 8.6** GPUs (for example NVIDIA MX570 A):

```bash
docker run --gpus all --shm-size 1g -p 8080:80 -v "$(pwd)/tei-data:/data" --name tei-e5 --pull always ghcr.io/huggingface/text-embeddings-inference:86-1.9 --model-id intfloat/multilingual-e5-small --dtype float16
```

> On Windows **CMD**, use `%cd%` instead of `$(pwd)`:

```cmd
docker run --gpus all --shm-size 1g -p 8080:80 -v "%cd%\tei-data:/data" --name tei-e5 --pull always ghcr.io/huggingface/text-embeddings-inference:86-1.9 --model-id intfloat/multilingual-e5-small --dtype float16
```

For other NVIDIA GPUs, use the corresponding TEI image for the GPU architecture.

---

## 2. Windows / Linux without NVIDIA GPU

Use the CPU image:

```bash
docker run -p 8080:80 -v "$(pwd)/tei-data:/data" --name tei-e5 --pull always ghcr.io/huggingface/text-embeddings-inference:cpu-1.9 --model-id intfloat/multilingual-e5-small
```

### Windows CMD

```cmd
docker run -p 8080:80 -v "%cd%\tei-data:/data" --name tei-e5 --pull always ghcr.io/huggingface/text-embeddings-inference:cpu-1.9 --model-id intfloat/multilingual-e5-small
```

---

## 3. macOS

### Apple Silicon (M1 / M2 / M3 / M4 / ...)

Use the ARM64 CPU image:

```bash
docker run -p 8080:80 -v "$(pwd)/tei-data:/data" --name tei-e5 --pull always ghcr.io/huggingface/text-embeddings-inference:cpu-arm64-1.9 --model-id intfloat/multilingual-e5-small
```

### Intel Mac

Use the x86_64 CPU image:

```bash
docker run -p 8080:80 -v "$(pwd)/tei-data:/data" --name tei-e5 --pull always ghcr.io/huggingface/text-embeddings-inference:cpu-1.9 --model-id intfloat/multilingual-e5-small
```

---

## 4. Stop / Start

Stop:

```bash
docker stop tei-e5
```

Start:

```bash
docker start tei-e5
```

Remove:

```bash
docker rm -f tei-e5
```

---

## Notes

- TEI uses port `8080` on the host.
- The model is downloaded automatically on the first run.
- `tei-data` is mounted to `/data`.
- CPU inference works without an NVIDIA GPU.
- Apple Silicon should use the ARM64 image.

Official documentation:

- [TEI supported hardware](https://huggingface.co/docs/text-embeddings-inference/supported_models)
- [TEI with GPU](https://huggingface.co/docs/text-embeddings-inference/local_gpu)
- [TEI with CPU](https://huggingface.co/docs/text-embeddings-inference/local_cpu)
- [TEI with Metal](https://huggingface.co/docs/text-embeddings-inference/local_metal)
