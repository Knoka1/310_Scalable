//
// PhotoApp helper functions
//
// Initial author:
//   Prof. Joe Hummel
//   Northwestern University
//

const fs = require('fs');
const ini = require('ini');
const config = require('./config.js');
const mysql2 = require('mysql2/promise');
const { RekognitionClient } = require('@aws-sdk/client-rekognition');
const { S3Client } = require('@aws-sdk/client-s3');
const { fromIni } = require('@aws-sdk/credential-providers');


/** 
 * async get_dbConn
 *
 * @description Reads config info and opens connection to MySQL server.
 * Returns dbConn object to use for executing queries against the
 * database; you should close the connection via .end() when done.
 * Throws an exception if an error occurs.
 *
 * NOTE: this is an async function, which returns a promise. You
 * must await return_value to properly wait for the connection to
 * be established and resolve the promise. 
 *
 * @param none
 * @returns {Promise<Connection>} dbConn connection object
 * @throws {Exception} if an error occurs
 */
async function get_dbConn()
{
  const config_data = fs.readFileSync(config.photoapp_config_filename, 'utf-8');
  const photoapp_config = ini.parse(config_data);
  const endpoint = photoapp_config.rds.endpoint;
  const port_number = photoapp_config.rds.port_number;
  const user_name = photoapp_config.rds.user_name;
  const user_pwd = photoapp_config.rds.user_pwd;
  const db_name = photoapp_config.rds.db_name;

  //
  // creates and open connection to MySQL server:
  //
  let dbConn = mysql2.createConnection(
    {
      host: endpoint,
      port: port_number,
      user: user_name,
      password: user_pwd,
      database: db_name,
      multipleStatements: true  // allow multiple queries in one call
    }
  );
  
  return dbConn;
}


/** 
 * sync get_bucket
 *
 * @description Reads config info and returns an object you can use
 * to interact with S3 storage service. No need to explicitly open 
 * or close the connection to S3. Throws an exception on an error.
 *
 * NOTE: this is a synchronous function using AWS's boto library,
 * no need to await for a connection to be established.
 *
 * @param none
 * @returns {Bucket} S3 bucket object
 * @throws {Exception} if an error occurs
 */
function get_bucket()
{
  const config_data = fs.readFileSync(config.photoapp_config_filename, 'utf-8');
  const photoapp_config = ini.parse(config_data);
  const s3_region_name = photoapp_config.s3.region_name;

  let bucket = new S3Client({
    region: s3_region_name,
    maxAttempts: 3,
    defaultsMode: "standard",
    credentials: fromIni({ profile: config.photoapp_s3_profile })
  });

  return bucket;
}


/** 
 * sync get_bucket_name
 *
 * @description Reads config info and returns the name of the 
 * S3 bucket. Throws an exception on an error.
 *
 * NOTE: this is a synchronous function using AWS's boto library,
 * no need to await for a connection to be established.
 *
 * @param none
 * @returns {string} S3 bucket name
 * @throws {Exception} if an error occurs
 */
function get_bucket_name()
{
  const config_data = fs.readFileSync(config.photoapp_config_filename, 'utf-8');
  const photoapp_config = ini.parse(config_data);
  const s3_bucket_name = photoapp_config.s3.bucket_name;

  return s3_bucket_name;
}


/** 
 * sync get_rekognition
 *
 * @description Reads config info and returns an object you can use
 * to call the Rekognition image analysis service. No need to 
 * explicitly open or close the connection to Rekognition. Throws
 * an exception on an error.
 *
 * NOTE: this is a synchronous function using AWS's boto library,
 * no need to await for a connection to be established.
 *
 * @param none
 * @returns {Rekognition} Rekognition object
 * @throws {Exception} if an error occurs
 */
function get_rekognition()
{
  const config_data = fs.readFileSync(config.photoapp_config_filename, 'utf-8');
  const photoapp_config = ini.parse(config_data);
  const s3_region_name = photoapp_config.s3.region_name;

  let rekognition = new RekognitionClient({
    region: s3_region_name,
    maxAttempts: 3,
    defaultsMode: "standard",
    credentials: fromIni({ profile: config.photoapp_s3_profile })
  });

  return rekognition;
}


//
// list the functions we are exporting:
//
module.exports = { get_dbConn, get_bucket, get_bucket_name, get_rekognition };
