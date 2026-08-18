import { CloudflareR2Storage } from "#@/infrastructure/storage/r2.service.js";
import { MediaService } from "./services/media.service.js";
import { MediaController } from "./controllers/media.controller.js";

class MediaContainer {
    public storageService = new CloudflareR2Storage();
    public mediaService = new MediaService(this.storageService);
    public mediaController = new MediaController(this.mediaService);
}

export const mediaContainer = new MediaContainer();