import { Client } from '@elastic/elasticsearch'; 
import { config } from '#@/config/config.js'; 

export const esClient = new Client({
    node: config.elasticsearch.node, 
});

export const checkElasticsearchConnection = async () => {
    try {
        const ping = await esClient.ping(); 
        if (ping) {
            console.log('✅ Successfully connected to Elasticsearch'); 
        } else {
            console.error('❌ Failed to connect to Elasticsearch'); 
        }
    } catch (error) {
        console.error('❌ Error connecting to Elasticsearch:', error); 
    }
};
