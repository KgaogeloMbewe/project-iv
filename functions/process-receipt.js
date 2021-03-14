const amqp = require('amqplib');

const url = "amqps://xrgawglz:9bwYNG_BuoeVu10TBSY70fW6trZdHVVp@rattlesnake.rmq.cloudamqp.com/xrgawglz";

exports.handler = async () => {
    try {
        //TODO add logic to pull items from queue & process them

        return {
            statusCode: 200,
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify('Successful'),
        };
    } catch (error) {
        console.log(error);

        return {
            statusCode: 500,
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify('An Error occurred'),
        }
    }
};