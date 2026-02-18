//
// API function: delete /images
//
// Deletes all images and associated labels from the database and S3.
//
// Author:
//   Prof. Joe Hummel
//   Northwestern University
//

const mysql2 = require('mysql2/promise');
const { get_dbConn, get_bucket, get_bucket_name } = require('./helper.js');
const { DeleteObjectsCommand } = require('@aws-sdk/client-s3');

//
// p_retry requires the use of a dynamic import:
// const pRetry = require('p-retry');
//
const pRetry = (...args) => import('p-retry').then(({default: pRetry}) => pRetry(...args));


/**
*
delete_images
:
*
*
@description
d
elete
s
all images and associated labels from the
*
database and S3.
If successful, returns a JSON object of the
* form {message: string} where the message is "success". If an
* error occurs, message will carry the error message.
The images
*
are not deleted from S3 unless the database is successfully
*
cleared; if an error occurs either (a) there are
no changes or
*
(b) the database is cleared but there may be
one or more images
*
remaining in S3 (which has no negative effect since they have
*
unique names).
*
*
@param
none
*
@returns
JSON {message: string
}
*/
exports.delete_images = async (request, response) => {
  async function try_delete_images() {
    let dbConn = null;
    try {
      //
      // open connection to database:
      //
      dbConn = await get_dbConn();
      await dbConn.beginTransaction();

      const [rows] = await dbConn.execute(
        "SELECT bucketkey FROM assets"
      );

      console.log(`found ${rows.length} images to delete`);

      let sql = `
        SET foreign_key_checks = 0;
        TRUNCATE TABLE assetlabels;
        TRUNCATE TABLE assets;
        SET foreign_key_checks = 1;
        ALTER TABLE assets AUTO_INCREMENT = 1001;
      `;
      
      await dbConn.query(sql);
      await dbConn.commit();

      console.log("deleted from database, now deleting from S3...");

      const bucket = get_bucket();
        try {
          await bucket.send(new DeleteObjectsCommand({
            Bucket: get_bucket_name(),
            Delete: { Objects: rows.map(row => ({ Key: row.bucketkey })) }
          }));
        } catch (error) {
          console.log(`S3 deletion error: ${error.message}`);
        }

      return { message: "success" };
    }
    catch (err) {
      //
      // exception:
      //
      console.log("ERROR in try_delete_images:");
      console.log(err.message);

        try {
          await dbConn.rollback();
        } catch (rollbackErr) {
          console.log(`rollback error: ${rollbackErr.message}`);
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
    console.log("**Call to delete /images...");

    let rows = await pRetry(try_delete_images, {retries: 2});

    //
    // success, return data in JSON format:
    //
    console.log("success, sending response...");

    response.json({
      "message": "success",
    });
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
    response.status(500).json({
      "message": err.message
    });
  }

};
