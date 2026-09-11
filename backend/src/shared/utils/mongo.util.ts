import { Types } from "mongoose";

const string2ObjectID = (id: string | null | undefined): Types.ObjectId | null => {
    if (!id) return null;
    if(!Types.ObjectId.isValid(id)){
        console.error("Error happen with id: ", id)
        return null
    }
    return new Types.ObjectId(id)
}
const objectID2String = (objectID: Types.ObjectId): String | null => {
    if(!Types.ObjectId.isValid(objectID)){
        console.error("Error happen with id: ", objectID)
        return null
    }
    return objectID.toString()
}
export {
    string2ObjectID,
    objectID2String
}