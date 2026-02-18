//
// API function: get /image
//
// Downloads an image from S3.
//
// Author:
//   Prof. Joe Hummel
//   Northwestern University
//
const { GetObjectCommand } = require('@aws-sdk/client-s3');
const { get_dbConn, get_bucket, get_bucket_name } = require('./helper.js');

const pRetry = (...args) => import('p-retry').then(({default: pRetry}) => pRetry(...args));

/**
*
get
_image
:
*
*
@description
downloads
an image
denoted by the given asset id. If
* successful a JSON object of the form
{message:
string,
userid: int,
* local_filename: string, data
:
base64
-
encoded
string
}
is sent where
*
message is
"success" and the remaining values are set based on the
* image downloaded; data is image contents as base64
-
encoded
string.
*
If an error occurs,
a status code of 500 is sent where JSON object's
* message is the error message and
userid
is
-
1
; the other values are
* undefined.
An invalid
assetid
is
considered a client
-
side error,
*
resulting in a status code 400 with a message "no such
assetid
"
*
and a
userid
of
-
1
; the other values are undefined.
*
*
@param
assetid
(required URL parameter)
of image to
download
*
@returns
JSON {message: string,
userid: int, local_filename: string,
data
:
base64
-
encoded string
}
*/
exports.get_image = async (request, response) => {

  async function try_get_image() {
    let dbConn = null;
    try {
      const assetid = request.query.assetid;

      if (assetid === undefined) {
        const err = new Error("missing assetid");
        err.status = 400;
        throw err;
      }

      dbConn = await get_dbConn();
      await dbConn.beginTransaction();

      const lookup_sql = "SELECT userid, localname, bucketkey FROM assets WHERE assetid = ?";
      const [rows] = await dbConn.execute(lookup_sql, [assetid]);

      if (rows.length === 0) {
        const err = new Error("no such assetid");
        err.status = 400;
        throw err;
      }

      const userid = rows[0].userid;
      const localname = rows[0].localname;
      const bucketkey = rows[0].bucketkey;

      await dbConn.commit();

      console.log("downloading from S3...");
      const bucket = get_bucket();
      const command = new GetObjectCommand({
        Bucket: get_bucket_name(),
        Key: bucketkey
      });

      const s3_response = await bucket.send(command);

      const image_base64 = await s3_response.Body.transformToString('base64');

      console.log(`success, downloaded ${localname}`);

      return {
        userid: userid,
        local_filename: localname,
        data: image_base64
      };

    } catch (err) {
      console.log("ERROR in try_get_image:");
      console.log(err.message);

      if (dbConn) {
        try {
          await dbConn.rollback();
        } catch (rollbackErr) {
          console.log(`rollback error: ${rollbackErr.message}`);
        }
      }
      throw err;
    } finally {
      try { await dbConn.end(); } catch (ignore) {}
    }
  }

  //
  // retry logic:
  //
  try {
    console.log("**Call to get /image...");

    const result = await pRetry(try_get_image, { retries: 2 });

    response.json({
      "message": "success",
      "userid": result.userid,
      "local_filename": result.local_filename,
      "data": result.data
    });
  }
  catch (err) {
    console.log("ERROR:");
    console.log(err.message);

    if (err.status === 400 || err.message === 'no such assetid' || err.message === 'missing assetid') {
      response.status(400).json({
        "message": err.message,
        "userid": -1,
        "local_filename": undefined,
        "data": undefined
      });
    } else {
      response.status(500).json({
        "message": err.message,
        "userid": -1,
        "local_filename": undefined,
        "data": undefined
      });
    }
  }

};
