//
// API function: post /images
//
// Returns all the images in the database.
//
// Author:
//   Prof. Joe Hummel
//   Northwestern University
//
const {PutObjectCommand, DeleteObjectCommand} = require('@aws-sdk/client-s3');
const {DetectLabelsCommand} = require('@aws-sdk/client-rekognition');
const uuid = require('uuid');
const mysql2 = require('mysql2/promise');
const { get_dbConn, get_bucket, get_bucket_name, get_rekognition } = require('./helper.js');
//
// p_retry requires the use of a dynamic import:
// const pRetry = require('p-retry');
//
const pRetry = (...args) => import('p-retry').then(({default: pRetry}) => pRetry(...args));

/**
*
post_image
:
*
*
@description
uploads an image with a unique name to S3, allowing the
* same local file to be uploaded multiple times if desired. A record
* of this image is inserted into the database, and upon success a JSON
* object of the form
{message: ...,
assetid
: ...}
is sent
where message is
*
"success"
and assetid is the unique id for this image in the database.
*
The image is also analyzed by the Rekognition AI service to label
*
objects within
t
he image; the results of this analysis are also saved
*
in the
database (and can be retrieved later via
GET /
image_labels).
*
If an error occurs,
a status code of 500 is
sent
where
the JSON
object's
*
message
is
the error
message and assetid
is
-
1.
An invalid userid
is
* considered a client
-
side error, resulting in a status code 400 with
* a message "no such userid" and an assetid of
-
1.
*
*
@param
userid
(required URL parameter)
for whom we are uploading this image
*
@param
request body {local_filename: string, data: base64
-
encoded string}
*
@returns
JSON {message: string,
assetid
:
int
}
*/
exports.post_image = async (request, response) => {

  async function get_username(dbConn, userid) {
    const lookup_user_sql = "SELECT username FROM users WHERE userid = ?";
    let [userRow] = await dbConn.execute(lookup_user_sql, [userid]);

    if (userRow.length === 0) {
      const err = new Error("no such userid");
      err.status = 400;
      throw err;
    }
    return userRow[0].username;
  }


  async function upload_to_s3(bucketkey, image_bytes) {
    const bucket = get_bucket();
    const parameters = {
      Bucket: get_bucket_name(),
      Key: bucketkey,
      Body: image_bytes,
    };
    let command = new PutObjectCommand(parameters);
    await bucket.send(command);
  }


  async function analyze_and_store_labels(dbConn, bucketkey, assetid) {
    const rekognition = get_rekognition();
    let rek_parameters = {
      Image: {
        S3Object: { Bucket: get_bucket_name(), Name: bucketkey },
      },
      MaxLabels: 100,
      MinConfidence: 80.0,
    };
    
    let rek_command = new DetectLabelsCommand(rek_parameters);
    let rek_response = await rekognition.send(rek_command);

    for (let label of rek_response.Labels) {
      let label_sql = "INSERT INTO assetlabels (assetid, label, confidence) VALUES (?, ?, ?);";
      await dbConn.execute(label_sql, [assetid, label.Name, label.Confidence]);
    }
  }

  async function try_post_image()
  {
    let dbConn = null;
    let s3_uploaded_success = false;
    let bucketkey = "";

    try {
      const userid = request.query.userid;
      let local_filename = request.body.local_filename;

      if (request.body.data === undefined) {
        const err = new Error("missing data");
        err.status = 400;
        throw err;
      }

      if (local_filename === undefined || local_filename === "") {
        local_filename = "untitled.jpg";
      }
      //
      // open connection to database:
      //
      dbConn = await get_dbConn();
      await dbConn.beginTransaction();

      const username = await get_username(dbConn, userid);
      const unique_str = uuid.v4();
      bucketkey = `${username}/${unique_str}-${local_filename}`;
      
      const image_bytes = Buffer.from(request.body.data, 'base64');

      console.log("uploading to S3...");
      await upload_to_s3(bucketkey, image_bytes);
      s3_uploaded_success = true;

      let insert_sql = "INSERT INTO assets (userid, localname, bucketkey) VALUES (?, ?, ?)";
      let [result, _] = await dbConn.execute(insert_sql, [userid, local_filename, bucketkey]);
      const assetid = result.insertId;

      console.log("analyzing image...");
      await analyze_and_store_labels(dbConn, bucketkey, assetid);

      console.log(`done, inserted asset ${assetid}`);
      await dbConn.commit();

      return {message: "success", assetid: assetid};
    }
    catch (err) {
      //
      // exception:
      //
      console.log("ERROR in try_post_image:");
      console.log(err.message);
      
      await dbConn.rollback()

      if (s3_uploaded_success) {
        try {
          const bucket = get_bucket();
          const deleteParams = { Bucket: get_bucket_name(), Key: bucketkey };
          await bucket.send(new DeleteObjectCommand(deleteParams));
        } catch (cleanupErr) {
          console.log("Cleanup ERROR:", cleanupErr.message);
        }
      }
      throw err;  // re-raise exception to trigger retry mechanism

    }
    finally {
      //
      // close connection:
      //
        try { await dbConn.end(); } catch(err) { /*ignore*/ }
    }
  }

  //
  // retry the inner function at most 3 times:
  //
  try {
    console.log("**Call to post /images...");

    let result = await pRetry(try_post_image, {retries: 2});

    //
    // success, return data in JSON format:
    //
    console.log("success, sending response...");

    response.json(result);
  }
  catch (err) {
    //
    // exception:
    //
    console.log("ERROR:");
    console.log(err.message);

    //
    // if an error occurs it's our fault, so use status code
    // of 500 => server-side error:
    //
    if (err.status === 400 || err.message === 'no such userid' || err.message === 'missing data') {
      response.status(400).json({
        "message": err.message,
        "assetid": -1
      });
    } else {
      response.status(500).json({
        "message": err.message,
        "assetid": -1
      });
    }
  }

};
