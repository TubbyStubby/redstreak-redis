import { RedisClientType, createClient } from "redis";
import { PubSub, PubSubCb } from "redstreak";

export class RedStreakRedis implements PubSub {
    #publisher: RedisClientType; #subscriber: RedisClientType;
    #subscribed: Set<string>;

    constructor(connection: string | RedisClientType) {
        if(typeof connection == "string") {
            this.#subscriber = createClient({ url: connection });
            this.#publisher = createClient({ url: connection });
        } else {
            this.#subscriber = connection.duplicate();
            this.#publisher = connection.duplicate();
        }
        if(!this.#publisher.isReady) this.#publisher.connect();
        if(!this.#subscriber.isReady) this.#subscriber.connect();
        this.#subscribed = new Set();
    }

    async subscribe(channel: string, cb: PubSubCb): Promise<void> {
        if(this.#subscribed.has(channel)) throw new Error(`Already subscribed to ${channel}`);
        this.#subscriber.subscribe(channel, cb);
    }

    async publish(channel: string, message: string): Promise<void> {
        this.#publisher.publish(channel, message);
    }
}
