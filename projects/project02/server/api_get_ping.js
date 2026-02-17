//
// API function: get /ping
//
// Returns (M, N) where M = # of items in the S3 bucket and 
// N = # of users in the database. The format of the response
// is {message: ..., M: ..., N: ...} where message is either 
// "success" or an error message (with status code 500). If 
// message is an error message, then M and N will be -1.
//
// Author:
//   Prof. Joe Hummel
//   Northwestern University
//

const mysql2 = require('mysql2/promise');
const { get_dbConn, get_bucket, get_bucket_name } = require('./helper.js');
const { ListObjectsV2Command } = require('@aws-sdk/client-s3');
//
// p_retry requires the use of a dynamic import:
// const pRetry = require('p-retry');
//
const pRetry = (...args) => import('p-retry').then(({default: pRetry}) => pRetry(...args));


/**
 * get_ping:
 * 
 * @description returns (M, N) where M = # of items in the bucket and 
 * N = # of users in the database. The format of the response is 
 * {message: ..., M: ..., N: ...} where message is either "success" or
 * an error message (with status code 500). If message is an error 
 * message, then M and N will be -1.
 *
 * @param none
 * @returns JSON {message: string, M: integer, N: integer} 
 */
exports.get_ping = async (request, response) => {

  async function get_M()
  {
    //
    // call out to S3 and get the items in the bucket...
    //
    let bucket = get_bucket();

    let parameters = {
      Bucket: get_bucket_name(),
    };

    console.log("calling S3...");

    let command = new ListObjectsV2Command(parameters);
    let promise_s3 = bucket.send(command); 

    return promise_s3;
  }

  async function get_N()
  {
    try {
      //
      // open connection to database and start query execution to
      // count # of users:
      //
      dbConn = await get_dbConn();

      let sql = `
                SELECT count(userid) AS num_users 
                FROM users;
                `;

      console.log("executing SQL...");
      
      let promise_mysql = dbConn.execute(sql);

      return promise_mysql;
    }
    finally {
      //
      // close connection:
      //
      try { await dbConn.end(); } catch(err) { /*ignore*/ }
    }
  }

  try {
    console.log("**Call to get /ping...");

    let promise_s3 = get_M();
    let promise_mysql = await pRetry(get_N, {retries: 2});

    //
    // wait for database and S3 to complete, if an error occurs
    // then an exception is thrown:
    //
    let results = await Promise.all([promise_s3, promise_mysql]);

    //
    // calls have completed, extract results from promises:
    console.log("operations completed...");

    //
    // S3 call is returning a list of the objects in the bucket,
    // so we return the length of the list:
    //
    let results_s3 = results[0];
    let contents = results_s3["Contents"];
    let M = contents.length;
    
    //
    // SQL is returning count of # of users, so extract from
    // result set:
    //
    let results_mysql = results[1];
    let [rows, _] = results_mysql;
    let row = rows[0];  // we only have one result back: num_users
    let N = row["num_users"];

    console.log(`M: ${M}`);
    console.log(`N: ${N}`);

    //
    // success:
    //  
    console.log("sending response...");

    response.json({
      "message": "success",
      "M": M,
      "N": N,
    });

  }
  catch (err) {
    //
    // exception:
    //
    console.log("ERROR:");
    console.log(err.message);

    //
    // if an error occurs it's our fault, so we use status code
    // 500 => server-side error:
    //
    response.status(500).json({
      "message": err.message,
      "M": -1,
      "N": -1,
    });
  }

};
