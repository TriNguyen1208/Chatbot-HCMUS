import crypto from "crypto";

function sha256(value: string): string {
    return crypto.createHash("sha256").update(value).digest("hex");
}
 
const RT_PREFIX = "rt:";

export {
    sha256,
    RT_PREFIX
}