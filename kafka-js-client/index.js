const kafka = require('kafka-node');


const client = new kafka.KafkaClient({ kafkaHost: '127.0.0.1:9092' });
const topcisToSubscribe = [{ topic: 'WordsWithCountsTopic' }];
const options = { autoCommit: false, groupId: 'test-consumer-group'};

const consumer = new kafka.Consumer(client, topcisToSubscribe, options);

consumer.on('message', function (message) {
    console.log('--------------------')
    console.log(`"${message.key}" showed up ${message.value} times`);
});

consumer.on('error', function (err) {
    console.log('Error was: ', err)
});

console.log('succesfully started consumer');