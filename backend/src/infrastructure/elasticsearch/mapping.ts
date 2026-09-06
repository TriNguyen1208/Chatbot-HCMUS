import { esClient } from './index.js'; 
import mongoose from 'mongoose';
import { UserModel } from '#@/modules/user/entities/user.entity.js';
import { ConversationModel } from '#@/modules/conversation/entities/conversation.entity.js';
import { MessageModel } from '#@/modules/message/entities/message.entity.js';

const USERS_INDEX = 'users';
const CONVERSATIONS_INDEX = 'conversations';
const MESSAGES_INDEX = 'messages';

const autocompleteSettings = {
    analysis: {
        analyzer: {
            autocomplete_analyzer: {
                type: 'custom' as const,
                tokenizer: 'autocomplete_tokenizer',
                filter: ['lowercase'],
            },
        },
        tokenizer: {
            autocomplete_tokenizer: {
                type: 'edge_ngram' as const,
                min_gram: 1,
                max_gram: 20,
                token_chars: ['letter' as const, 'digit' as const],
            },
        },
    },
};

const syncDataToES = async (indexName: string, model: mongoose.Model<any>) => {
    try {
        const docs = await model.find({}).lean();
        if (docs.length > 0) {
            const operations = docs.flatMap(doc => {
                const id = doc._id.toString();
                const { _id, ...docData } = doc as Record<string, any>;
                return [
                    { index: { _index: indexName, _id: id } },
                    { id, ...docData }
                ];
            });
            await esClient.bulk({ refresh: true, operations });
            console.log(`Synced ${docs.length} documents to ${indexName}`);
        }
    } catch (error) {
        console.error(`Error syncing data to ${indexName}:`, error);
    }
};

export const initializeIndices = async () => {
    try {
        // --- 1. Tạo index cho Users ---
        const usersExists = await esClient.indices.exists({ index: USERS_INDEX }); 
        if (!usersExists) { 
            await esClient.indices.create({
                index: USERS_INDEX,
                settings: autocompleteSettings,
                mappings: { 
                    properties: {
                        id: { type: 'keyword' }, 
                        mssv: { type: 'text', analyzer: 'autocomplete_analyzer', search_analyzer: 'standard' }, 
                        name: { type: 'text', analyzer: 'autocomplete_analyzer', search_analyzer: 'standard' },
                        email: { type: 'text', analyzer: 'autocomplete_analyzer', search_analyzer: 'standard' },
                        phone: { type: 'text', analyzer: 'autocomplete_analyzer', search_analyzer: 'standard' },
                    },
                },
            });
            console.log(`Created index: ${USERS_INDEX}`); 
            await syncDataToES(USERS_INDEX, UserModel);
        }

        // --- 2. Tạo index cho Conversations ---
        const conversationsExists = await esClient.indices.exists({ index: CONVERSATIONS_INDEX });
        if (!conversationsExists) {
            await esClient.indices.create({
                index: CONVERSATIONS_INDEX,
                settings: autocompleteSettings,
                mappings: {
                    properties: {
                        id: { type: 'keyword' }, 
                        name: { type: 'text', analyzer: 'autocomplete_analyzer', search_analyzer: 'standard' },
                        member_ids: { type: 'keyword' },
                    },
                },
            });
            console.log(`Created index: ${CONVERSATIONS_INDEX}`);
            await syncDataToES(CONVERSATIONS_INDEX, ConversationModel);
        }

        // --- 3. Tạo index cho Messages ---
        const messagesExists = await esClient.indices.exists({ index: MESSAGES_INDEX });
        if (!messagesExists) {
            await esClient.indices.create({
                index: MESSAGES_INDEX,
                settings: autocompleteSettings,
                mappings: {
                    properties: {
                        id: { type: 'keyword' }, 
                        conversation_id: { type: 'keyword' }, 
                        sender_id: { type: 'keyword' }, 
                        content: { type: 'text', analyzer: 'autocomplete_analyzer', search_analyzer: 'standard' }, 
                    },
                },
            });
            console.log(`Created index: ${MESSAGES_INDEX}`);
            await syncDataToES(MESSAGES_INDEX, MessageModel);
        }
    } catch (error) {
        console.error('Error initializing Elasticsearch indices:', error); 
    }
};
