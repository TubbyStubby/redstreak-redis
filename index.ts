import { RedisClientOptions, RedisClientType, createClient } from "redis";
import { PubSub, PubSubCb } from "redstreak";

type RedStreakRedisConnectionOption = {
    clientOptions: RedisClientOptions,
    subscriber?: never,
    publisher?: never
}

type RedStreakRedisClientOption = {
    options?: never,
    subscriber: RedisClientType,
    publisher: RedisClientType
}

export class RedStreakRedis implements PubSub {
    #clientType: "self" | "provided";
    #publisher; #subscriber;
    #subscribed: Set<string>;

    constructor(options: RedStreakRedisConnectionOption | RedStreakRedisClientOption) {
        if(options.subscriber && options.publisher) {
            this.#clientType = "provided";
            this.#subscriber = options.subscriber;
            this.#publisher = options.publisher;
        } else {
            this.#clientType = "self";
            this.#subscriber = createClient(options.clientOptions);
            this.#publisher = createClient(options.clientOptions);
        }
        if(!this.#publisher.isReady) this.#publisher.connect();
        if(!this.#subscriber.isReady) this.#subscriber.connect();
        this.#subscribed = new Set();
    }

    async subscribe(channel: string, cb: PubSubCb): Promise<void> {
        if(this.#subscribed.has(channel)) throw new Error(`Already subscribed to ${channel}`);
        this.#subscriber.subscribe(channel, cb);
        this.#subscribed.add(channel);
    }

    async publish(channel: string, message: string): Promise<void> {
        this.#publisher.publish(channel, message);
    }

    async unsubscribe(channel?: string) {
        if(this.#subscribed.size == 0) return;
        if(channel) {
            if(!this.#subscribed.has(channel)) return;
            await this.#subscriber.unsubscribe(channel);
            this.#subscribed.delete(channel);
        } else {
            const channels = [...this.#subscribed];
            await this.#subscriber.unsubscribe(channels);
            this.#subscribed.clear();
        }
    }

    async disconnect() {
        if(this.#clientType == "provided") return;
        await this.#subscriber.disconnect();
        await this.#publisher.disconnect();
    }
}
