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

const consumer = kafka.consumer({ groupId: `${process.env.KAFKA_CONSUMER_GROUP_ID}_${process.env.ENV_NAME}`});

const runConsumer = async () => {
    await consumer.connect();
    await consumer.subscribe({
        topic: process.env.KAFKA_TOPIC_NAME,
        fromBeginning: true
    })

    await consumer.run({
        eachMessage: ({ topic, partition, message}) => {
            const convertedmessage = message.value.toString('utf-8');
            console.log(`Receive message from topic ${topic}, partition: ${partition},message: ${convertedmessage}`)
        }
    })
}

runConsumer().then().catch(e => console.log(e));