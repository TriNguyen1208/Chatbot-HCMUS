import { fastWorker } from "./fast.worker.js";
import { mediaWorker } from "./media.worker.js";
import { cronWorker } from "./cron.worker.js";

export { fastWorker, mediaWorker, cronWorker };
export const workers = [fastWorker, mediaWorker, cronWorker];
export default fastWorker;
