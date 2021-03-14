require('dotenv').config();

const amqp = require('amqplib');
const process = require('process');

const url = process.env.RABBITMQ_URL;

exports.handler = async () => {
    try {
        //TODO add logic to pull items from queue & process them

        return {
            statusCode: 200,
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({'msg': 'Successful'}),
        };
    } catch (error) {
        console.log(error);

        return {
            statusCode: 500,
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({'error': 'An Error occurred'}),
        }
    }
};