require('dotenv').config();

const amqp = require('amqplib');
const process = require('process');

const url = process.env.RABBITMQ_URL;

const mysql = require('mysql2/promise');

exports.handler = async () => {
    try {
        const conn = await amqp.connect(url);
        const channel = await conn.createChannel();
        const queue = 'processed-data'; //TODO check if needs rename

        await channel.assertQueue(queue, {durable: true});
        await channel.prefetch(1);

        console.log('[*_o] Waiting for messages in the %s. queue', queue);

        await channel.consume(queue, async (data) => {
            //TODO Add logic to insert the processed receipt into data warehouse
            const msg = JSON.parse(data.content);

            console.log(msg);

            // const conn = await mysql.createConnection({
            //     user: 'db-user-1',
            //     password: 'db_user-1',
            //     database: 'project_iv',
            //     host: '127.0.0.1',
            //     port: '5431',
            // });

            // const result = await conn.query(
            //     'INSERT INTO `payment_options` (`pay_option_desc`) VALUES (?)', ['debit']
            // );
    
            // console.log(result[0]);
            
            // await conn.end();

            // channel.ack(data);
        }, {noAck: false});

        await channel.close();
        await conn.close();

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
        };
    }
};

async function getPaymentOption (conn, data) {
    const paymentDesc = (data.paymentInfo.length > 0 && data.paymentInfo.customerMuskedCardNumber) ? 'card' : 'cash';
    const q = 'SELECT `pay_option_id` FROM `payment_options` where `pay_option_desc` = ? ';
    const qResult = await conn.query(q, [paymentDesc]);

    if (qResult[0].length && qResult[0][0]) {
        return qResult[0][0].pay_option_id;
    }

    const i = 'INSERT INTO `payment_options` (`pay_option_desc`) VALUES (?)';
    const iResult = await conn.execute(i, [paymentDesc]);

    return iResult.insertId;
}

async function getStore (conn, data) {
    const q = 'SELECT `store_id` FROM `store_details` WHERE `store_name` = ? and `store_contact` = ?';
    const qResult = await conn.query(q, [data.storeInfo.storeName, data.storeInfo.storeContact]);

    if (qResult[0].length && qResult[0][0]) {
        return qResult[0][0].store_id;
    }

    const i = 'INSERT INTO `store_details`\
     (`store_name`, `store_street_name`, `store_city`, `store_province`, `store_zip_code`, `store_contact`)\
      VALUES\
       (?, ?, ?, ?, ?, ?)';
    const iResult = await conn.execute(i, [
        data.storeName, 
        data.storeStreetName,
        data.storeCity, 
        data.storeProvince, 
        data.storeZipCode,
        data.storeContact
    ]);

    return iResult.insertId;
}

async function getProduct (conn, data) {
    const q = 'SELECT `product_id` FROM `products` WHERE `product_name` = ? and `store_id` = ?';
    const qResult = await conn.query(q, [data.productName, data.storeId]);

    if (qResult[0].length && qResult[0][0]) {
        return qResult[0][0].product_id;
    }

    const i = 'INSERT INTO `products`\
     (`product_name`, `product_desc`, `product_price`, `store_id`)\
      VALUES\
       (?, ?, ?, ?)';
    const iResult = await conn.execute(i, [
        data.productName, 
        data.productDescription,
        data.productPrice, 
        data.storeId
    ]);

    return iResult.insertId;
}
