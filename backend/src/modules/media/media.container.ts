import { CloudflareR2Storage } from "#@/infrastructure/storage/r2.service.js";
import { MediaService } from "./services/media.service.js";
import { MediaController } from "./controllers/media.controller.js";

class MediaContainer {
    private _storageService?: CloudflareR2Storage;
    public get storageService() {
        if (!this._storageService) {
            this._storageService = new CloudflareR2Storage();
        }
        return this._storageService;
    }
    
    private _mediaService?: MediaService;
    public get mediaService() {
        if (!this._mediaService) {
            this._mediaService = new MediaService(this.storageService);
        }
        return this._mediaService;
    }

    private _mediaController?: MediaController;
    public get mediaController() {
        if (!this._mediaController) {
            this._mediaController = new MediaController(this.mediaService);
        }
        return this._mediaController;
    }
}

export const mediaContainer = new MediaContainer();