import amqp from 'amqplib'; 
import type { ChannelModel, Channel } from 'amqplib';
import { config } from '#@/config/config.js';

class RabbitMQConnection {
    private connection: ChannelModel | null = null; 
    private channel: Channel | null = null;      

    async connect(): Promise<Channel> {
        if (this.channel) return this.channel; 

        try {
            this.connection = await amqp.connect(config.rabbitmq.uri);
            this.channel = await this.connection.createChannel();
            console.log('✅ Successfully connected to RabbitMQ');

            await this.channel.assertQueue('sync_user_es', { durable: true });
            await this.channel.assertQueue('sync_conversation_es', { durable: true });
            await this.channel.assertQueue('sync_message_es', { durable: true });
            await this.channel.assertQueue('sync_watermark_db', { durable: true });

            return this.channel;
        } catch (error) {
            console.error('❌ Error connecting to RabbitMQ:', error);
            throw error; 
        }
    }

    getChannel(): Channel {
        if (!this.channel) {
            throw new Error('RabbitMQ channel not initialized. Call connect() first.'); // Báo lỗi nếu gọi mà chưa connect
        }
        return this.channel;
    }

    async close() {
        if (this.channel) await this.channel.close();
        if (this.connection) await this.connection.close();
    }
}

export const rabbitmq = new RabbitMQConnection();
