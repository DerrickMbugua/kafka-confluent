const {Kafka} = require('kafkajs');
const dotenv = require('dotenv');

dotenv.config({ path: './.env' });

const kafka = new Kafka({
    clientId: process.env.KAFKA_CLIENT_ID,
    brokers: [process.env.KAFKA_BROKERS],
    sasl: {
        mechanism: 'plain',
        username: process.env.KAFKA_USERNAME,
        password: process.env.KAFKA_PASSWORD
    },
    autoConnect: true,
    connectionTimeout: 1000000,
    ssl: process.env.KAFKA_SSL
});

const producer = kafka.producer();

const run = async () => {
    await producer.connect();

    const payload = {
        id: "Demo101",
        price: "999.9",
        description: "Item A description"
    }

    const outgoingMessages = [];
    outgoingMessages.push({
        key: payload.id,
        value: JSON.stringify(payload),
        headers: {
            product: 'Item A'
        }
    })

    const result = await producer.send({
        topic: process.env.KAFKA_TOPIC_NAME,
        messages: outgoingMessages
    });

    console.log(result);
}

run().catch(async (e) => {
    console.error(e);
    producer && (await producer.disconnect());
    process.exit();
})