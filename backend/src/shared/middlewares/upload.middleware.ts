import multer from "multer";

//Using main memory (RAM)
const storage = multer.memoryStorage();

const upload = multer({
    storage: storage,
    limits: {
        fileSize: 10 * 1024 * 1024,
    }
});

export const uploadMiddleware = upload.single("file");
