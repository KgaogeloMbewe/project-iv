require('dotenv').config();

const process = require('process');
const mysql = require('mysql2/promise');

exports.connectDb = async function () {
    return await mysql.createConnection({
        user: process.env.DB_USER,
        password: process.env.DB_PASS,
        database: process.env.DB_NAME,
        host: process.env.DB_HOST,
        port: process.env.DB_PORT,
    });
};

exports.getPaymentOption = async function (conn, data) {
    const paymentDesc = (data.cardNumber) ? 'card' : 'cash';
    const q = 'SELECT `pay_option_id` FROM `payment_options` where `pay_option_desc` = ? ';
    const qResult = await conn.query(q, [paymentDesc]);

    if (qResult[0].length && qResult[0][0]) {
        return qResult[0][0].pay_option_id;
    }

    const i = 'INSERT INTO `payment_options` (`pay_option_desc`) VALUES (?)';
    const iResult = await conn.execute(i, [paymentDesc]);

    return iResult[0].insertId;
};

exports.getStore =  async function (conn, data) {
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
        data.storeInfo.storeName, 
        data.storeInfo.storeStreetName,
        data.storeInfo.storeCity, 
        data.storeInfo.storeProvince, 
        data.storeInfo.storeZipCode,
        data.storeInfo.storeContact
    ]);

    return iResult[0].insertId;
};

exports.getSupplier = async function (conn, data) {
    const q = 'SELECT `supplier_id` FROM `suppliers` WHERE `supplier_name` = ? and `supplier_contact` = ?';
    const qResult = await conn.query(q, [data.productSupplierName, data.productSupplierContact]);

    if (qResult[0].length && qResult[0][0]) {
        return qResult[0][0].supplier_id;
    }

    const i = 'INSERT INTO `suppliers`\
     (`supplier_name`, `supplier_contact`, `supplier_location`)\
      VALUES\
       (?, ?, ?)';
    const iResult = await conn.execute(i, [
        data.productSupplierName, 
        data.productSupplierContact,
        data.productSupplierLocation
    ]);

    return iResult[0].insertId;
};

exports.getProduct = async function (conn, data, storeId) {
    const q = 'SELECT `product_id` FROM `products` WHERE `product_name` = ? and `store_id` = ?';
    const qResult = await conn.query(q, [data.productName, storeId]);

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
        parseFloat(data.productPrice), 
        storeId,
    ]);

    return iResult[0].insertId;
};

exports.createReceipt = async function (conn, data) {
    const i = 'INSERT INTO `receipts`\
     (`receipt_number`, `product_id`, `store_id`, `pay_option_id`, `supplier_id`, `price`)\
      VALUES\
       (?, ?, ?, ?, ?, ?)';
    const iResult = await conn.execute(i, [
        data.receiptNumber, 
        data.productId,
        data.storeId, 
        data.payOptionId, 
        data.supplierId,
        data.price,
    ]);

    return iResult[0].insertId;
};
