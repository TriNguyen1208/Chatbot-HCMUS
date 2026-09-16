import { esClient } from '#@/infrastructure/elasticsearch/es.client.js';
import type { Client } from '@elastic/elasticsearch';
import type { SearchResult } from './search.dto.js';

export class SearchService {
    constructor(private readonly client: Client = esClient) { }

    /**
     * Global Search across users, conversations, and messages.
     */
    async globalSearch(keyword: string, userId: string): Promise<SearchResult[]> {
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
                type: conv.type,
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

    /**
     * Search Users by name, mssv, email, or phone.
     */
    async searchUsers(keyword: string) {
        const { hits } = await this.client.search({
            index: 'users',
            query: {
                bool: {
                    should: [
                        {
                            multi_match: {
                                query: keyword,
                                fields: ['name^3', 'student_id^3', 'email^2', 'phone'],
                                type: 'phrase_prefix',
                                boost: 2
                            }
                        },
                        {
                            multi_match: {
                                query: keyword,
                                fields: ['name^2', 'email'],
                                fuzziness: 'AUTO'
                            }
                        }
                    ],
                    minimum_should_match: 1
                }
            }
        });

        return hits.hits.map((hit: any) => hit._source);
    }

    /**
     * Search Conversations where user is a member and name/members match.
     */
    async searchConversations(keyword: string, userId: string, matchedUsers?: any[]) {
        if (!matchedUsers) {
            matchedUsers = await this.searchUsers(keyword);
        }
        const matchedUserIds = matchedUsers.map((user: any) => user.id).filter(id => id !== userId);

        const { hits } = await this.client.search({
            index: 'conversations',
            query: {
                bool: {
                    filter: [
                        { term: { 'member_ids': userId } }
                    ],
                    should: [
                        {
                            match_phrase_prefix: {
                                name: {
                                    query: keyword,
                                    boost: 2
                                }
                            }
                        },
                        {
                            match: {
                                name: {
                                    query: keyword,
                                    fuzziness: 'AUTO'
                                }
                            }
                        },
                        ...(matchedUserIds.length > 0 ? [{ terms: { 'member_ids': matchedUserIds } }] : [])
                    ],
                    minimum_should_match: 1
                }
            }
        });

        const conversations = hits.hits.map((hit: any) => hit._source);

        const missingUserIds = new Set<string>();
        for (const conv of conversations) {
            if ((!conv.name || !conv.avatar_url) && Array.isArray(conv.member_ids)) {
                const otherId = conv.member_ids.find((id: string) => id !== userId);
                if (otherId) missingUserIds.add(otherId);
            }
        }

        if (missingUserIds.size > 0) {
            const userDocs = await this.client.mget({
                index: 'users',
                docs: Array.from(missingUserIds).map(id => ({ _id: id, _source: ['id', 'name', 'avatar_url'] }))
            });
            const userMap = new Map<string, any>();
            (userDocs.docs || []).forEach((doc: any) => {
                if (doc.found) userMap.set(doc._id, doc._source);
            });

            for (const conv of conversations) {
                if ((!conv.name || !conv.avatar_url) && Array.isArray(conv.member_ids)) {
                    const otherId = conv.member_ids.find((id: string) => id !== userId);
                    if (otherId && userMap.has(otherId)) {
                        const other = userMap.get(otherId);
                        if (!conv.name) conv.name = other.name;
                        if (!conv.avatar_url) conv.avatar_url = other.avatar_url;
                    }
                }
            }
        }

        return conversations;
    }

    /**
     * Search Messages within user's conversations.
     */
    async searchMessages(keyword: string, userId: string, conversationId?: string) {
        const filters: any[] = [];

        if (conversationId) {
            filters.push({ term: { conversation_id: conversationId } });
        } else {
            const userConversations = await this.client.search({
                index: 'conversations',
                _source: false,
                size: 1000,
                query: { term: { 'member_ids': userId } }
            });
            const userConvIds = userConversations.hits.hits.map(h => h._id);

            if (userConvIds.length > 0) {
                filters.push({ terms: { conversation_id: userConvIds } });
            } else {
                filters.push({ term: { conversation_id: 'none' } });
            }
        }

        const { hits } = await this.client.search({
            index: 'messages',
            query: {
                bool: {
                    filter: filters,
                    should: [
                        {
                            match_phrase_prefix: {
                                content: {
                                    query: keyword,
                                    boost: 2
                                }
                            }
                        },
                        {
                            match: {
                                content: {
                                    query: keyword,
                                    fuzziness: 'AUTO'
                                }
                            }
                        }
                    ],
                    minimum_should_match: 1
                }
            }
        });

        const messages = hits.hits.map((hit: any) => hit._source);

        const senderIds = Array.from(new Set(messages.map((m: any) => m.sender_id).filter(Boolean)));
        const convIds = Array.from(new Set(messages.map((m: any) => m.conversation_id).filter(Boolean)));

        const [sendersResult, convsResult] = await Promise.all([
            senderIds.length > 0 ? this.client.mget({
                index: 'users',
                docs: senderIds.map(id => ({ _id: id as string, _source: ['id', 'name', 'avatar_url'] }))
            }) : Promise.resolve({ docs: [] }),
            convIds.length > 0 ? this.client.mget({
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

        const missingConvUserIds = new Set<string>();
        for (const conv of convMap.values()) {
            if ((!conv.name || !conv.avatar_url) && Array.isArray(conv.member_ids)) {
                const otherId = conv.member_ids.find((id: string) => id !== userId);
                if (otherId && !senderMap.has(otherId)) {
                    missingConvUserIds.add(otherId);
                }
            }
        }

        if (missingConvUserIds.size > 0) {
            const extraUserDocs = await this.client.mget({
                index: 'users',
                docs: Array.from(missingConvUserIds).map(id => ({ _id: id, _source: ['id', 'name', 'avatar_url'] }))
            });
            (extraUserDocs.docs || []).forEach((doc: any) => {
                if (doc.found) senderMap.set(doc._id, doc._source);
            });
        }

        for (const conv of convMap.values()) {
            if ((!conv.name || !conv.avatar_url) && Array.isArray(conv.member_ids)) {
                const otherId = conv.member_ids.find((id: string) => id !== userId);
                if (otherId && senderMap.has(otherId)) {
                    const other = senderMap.get(otherId);
                    if (!conv.name) conv.name = other.name;
                    if (!conv.avatar_url) conv.avatar_url = other.avatar_url;
                }
            }
        }

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
