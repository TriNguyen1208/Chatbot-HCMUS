import { esClient } from '#@/infrastructure/elasticsearch/index.js';

export type SearchResult = {
    search_type: 'user' | 'conversation' | 'message';
    id: string;
    name?: string;
    avatar_url?: string;
    text?: string;
    sender?: {
        id?: string;
        name?: string;
        avatar_url?: string;
    };
    conversation?: {
        id?: string;
        name?: string;
        avatar_url?: string;
        type?: string;
    };
};

export class SearchService {
    // Tìm kiếm toàn cục (Global Search)
    static async globalSearch(keyword: string, userId: string): Promise<SearchResult[]> {
        const users = await this.searchUsers(keyword);
        
        const [conversations, messages] = await Promise.all([
            this.searchConversations(keyword, userId, users),
            this.searchMessages(keyword, userId)
        ]);

        const results: SearchResult[] = [];

        for (const user of users) {
            results.push({
                search_type: 'user',
                id: user.id,
                name: user.name,
                avatar_url: user.avatar_url,
            });
        }

        for (const conv of conversations) {
            results.push({
                search_type: 'conversation',
                id: conv.id,
                name: conv.name,
                avatar_url: conv.avatar_url,
            });
        }

        for (const msg of messages) {
            results.push({
                search_type: 'message',
                id: msg.id,
                text: msg.content,
                sender: msg.sender,
                conversation: msg.conversation
            });
        }

        const seen = new Set<string>();

        return results.filter((item) => {
            const key = `${item.search_type}_${item.id}`;
            if (seen.has(key)) return false;
            seen.add(key);
            return true;
        });
    }

    // Tìm kiếm Users
    static async searchUsers(keyword: string) {
        const { hits } = await esClient.search({
            index: 'users',
            query: {
                multi_match: { query: keyword, fields: ['name', 'mssv', 'email', 'phone'], type: 'phrase_prefix' }
            }
        });

        return hits.hits.map((hit: any) => hit._source);
    }

    static async searchConversations(keyword: string, userId: string, matchedUsers?: any[]) {
        // Bước 1: Tìm users match keyword (nếu chưa có)
        if (!matchedUsers) {
            matchedUsers = await this.searchUsers(keyword);
        }
        const matchedUserIds = matchedUsers.map((user: any) => user.id).filter(id => id !== userId);

        // Bước 2: Tìm conversation
        const { hits } = await esClient.search({
            index: 'conversations',
            query: {
                bool: {
                    filter: [
                        { term: { 'member_ids': userId } }
                    ],
                    should: [
                        { match_phrase_prefix: { name: keyword } },
                        ...(matchedUserIds.length > 0 ? [{ terms: { 'member_ids': matchedUserIds } }] : [])
                    ],
                    minimum_should_match: 1
                }
            }
        });

        return hits.hits.map((hit: any) => hit._source);
    }

    static async searchMessages(keyword: string, userId: string, conversationId?: string) {
        const filters: any[] = [];

        if (conversationId) {
            filters.push({ term: { conversation_id: conversationId } });
        } else {
            // Lấy danh sách conversation_id mà user này tham gia để lọc messages
            const userConversations = await esClient.search({
                index: 'conversations',
                _source: false, // Chỉ lấy id để tối ưu
                size: 1000, // Giới hạn số lượng hội thoại
                query: { term: { 'member_ids': userId } }
            });
            const userConvIds = userConversations.hits.hits.map(h => h._id);

            if (userConvIds.length > 0) {
                filters.push({ terms: { conversation_id: userConvIds } });
            } else {
                // User không tham gia conversation nào, không match message nào
                filters.push({ term: { conversation_id: 'none' } });
            }
        }

        const { hits } = await esClient.search({
            index: 'messages',
            query: {
                bool: {
                    must: [
                        { match_phrase_prefix: { content: keyword } }
                    ],
                    filter: filters
                }
            }
        });

        const messages = hits.hits.map((hit: any) => hit._source);

        const senderIds = Array.from(new Set(messages.map((m: any) => m.sender_id).filter(Boolean)));
        const convIds = Array.from(new Set(messages.map((m: any) => m.conversation_id).filter(Boolean)));

        const [sendersResult, convsResult] = await Promise.all([
            senderIds.length > 0 ? esClient.mget({
                index: 'users',
                docs: senderIds.map(id => ({ _id: id as string, _source: ['id', 'name', 'avatar_url'] }))
            }) : Promise.resolve({ docs: [] }),
            convIds.length > 0 ? esClient.mget({
                index: 'conversations',
                docs: convIds.map(id => ({ _id: id as string, _source: ['id', 'name', 'avatar_url', 'type', 'member_ids'] }))
            }) : Promise.resolve({ docs: [] })
        ]);

        const senderMap = new Map();
        (sendersResult.docs || []).forEach((doc: any) => {
            if (doc.found) senderMap.set(doc._id, doc._source);
        });

        const convMap = new Map();
        (convsResult.docs || []).forEach((doc: any) => {
            if (doc.found) convMap.set(doc._id, doc._source);
        });

        return messages.map((m: any) => {
            const sender = senderMap.get(m.sender_id);
            const conv = convMap.get(m.conversation_id);
            
            const { sender_id, conversation_id, ...restMessage } = m;

            return {
                ...restMessage,
                sender: sender ? {
                    id: sender.id,
                    name: sender.name,
                    avatar_url: sender.avatar_url
                } : undefined,
                conversation: conv ? {
                    id: conv.id,
                    name: conv.name,
                    avatar_url: conv.avatar_url,
                    type: conv.type
                } : undefined
            };
        });
    }
}
