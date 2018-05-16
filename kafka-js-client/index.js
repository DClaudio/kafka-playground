var kafka = require('kafka-node');
console.log('Starting kafka client');
const client = new kafka.KafkaClient({kafkaHost: '127.0.0.1:9092', groupId: 'test-consumer-group'});
console.log('succesfully connected to kafka');

const consumer = new kafka.Consumer(
    client,
    [
        { topic: 'WordsWithCountsTopic'}  
    ],
    {
        autoCommit: false
    }
);

consumer.on('message', function (message) {
    console.log(message);
});

consumer.on('error', function (err) {
    console.log('Error was: ', err)
});

console.log('succesfully configured consumer');