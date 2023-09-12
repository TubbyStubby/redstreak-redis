import { Redis, RedisOptions } from "ioredis";
import { PubSub, PubSubCb } from "redstreak";

type RedStreakRedisConnectionOption = {
    redisOptions: RedisOptions,
    subscriber?: never,
    publisher?: never
}

type RedStreakRedisClientOption = {
    redisOptions?: never,
    subscriber: Redis,
    publisher: Redis
}

export class RedStreakRedis implements PubSub {
    #clientType: "self" | "provided";
    #publisher; #subscriber;
    #subscribed: Map<string, PubSubCb>;

    constructor(options: RedStreakRedisConnectionOption | RedStreakRedisClientOption) {
        if(options.subscriber && options.publisher) {
            this.#clientType = "provided";
            this.#subscriber = options.subscriber;
            this.#publisher = options.publisher;
        } else {
            this.#clientType = "self";
            this.#subscriber = new Redis(options.redisOptions);
            this.#publisher = new Redis(options.redisOptions);
        }
        this.#subscribed = new Map();
        this.#subscriber.on("message", (channel, message) => this.#subscribed.get(channel)?.(message));
    }

    async subscribe(channel: string, cb: PubSubCb): Promise<void> {
        if(this.#subscribed.has(channel)) throw new Error(`Already subscribed to ${channel}`);
        this.#subscribed.set(channel, cb);
        await this.#subscriber.subscribe(channel);
    }

    async publish(channel: string, message: string): Promise<void> {
        await this.#publisher.publish(channel, message); 
    }

    async unsubscribe(channel?: string) {
        if(this.#subscribed.size == 0) return;
        if(channel) {
            if(!this.#subscribed.has(channel)) return;
            await this.#subscriber.unsubscribe(channel);
            this.#subscribed.delete(channel);
        } else {
            await this.#subscriber.unsubscribe(...this.#subscribed.keys());
            this.#subscribed.clear();
        }
    }

    async disconnect() {
        if(this.#clientType == "provided") return;
        await this.#subscriber.disconnect();
        await this.#publisher.disconnect();
    }
}
