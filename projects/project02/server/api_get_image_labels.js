//
// API function: get image_labels
//
// Downloads an image from S3.
//
// Author:
//   Prof. Joe Hummel
//   Northwestern University
//
const mysql2 = require('mysql2/promise');
const { get_dbConn } = require('./helper.js');

const pRetry = (...args) => import('p-retry').then(({default: pRetry}) => pRetry(...args));

/**
* get_
image_labels
*
*
@description
when
an image is uploaded to S3, the Rekognition
*
AI service is
automatically called to label objects in the image.
*
Given the image assetid, this function retrieves those labels.
* If successful the labels are returned as a JSON object of the
* form {message: ..., data: ...} where message is "success" and
* data is a list of dictionary
-
like objects of the form
* {"label": string, "confidence": int}, ordered by label. If
*
an error occurs,
status code of 500 is sent where JSON object's
* message is the error message and the list is empty []. An
* invalid assetid is considered a client
-
side error, resulting
* in status code 400 with a message "no such assetid" and an empty
*
list
[].
*
*
@param
assetid
(required URL parameter)
of image to retrieve labels for
*
@returns
JSON {message: string, data: [object, object, ...]}
*/
exports.get_image_labels = async (request, response) => {

  async function try_get_image_labels() {
    let dbConn = null;
    try {
      const assetid = request.params.assetid;

      if (assetid === undefined) {
        const err = new Error("missing assetid");
        err.status = 400;
        throw err;
      }

      dbConn = await get_dbConn();
      await dbConn.beginTransaction();

      const lookup_sql = "SELECT labelid, assetid, label, confidence FROM assetlabels WHERE assetid = ? ORDER BY label";
      const [rows] = await dbConn.execute(lookup_sql, [assetid]);

      if (rows.length === 0) {
        const err = new Error("no such assetid");
        err.status = 400;
        throw err;
      }

      console.log("getting labels...");

      await dbConn.commit();

      return {
        message: "success",
        data: rows.map(row => ({
          label: row.label,
          confidence: row.confidence
        }))
      };

    } catch (err) {
      console.log("ERROR in try_get_image_labels:");
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
    console.log("**Call to get image_labels...");

    const result = await pRetry(try_get_image_labels, { retries: 2 });

    response.json({
      "message": "success",
      "data": result.data
    });
  }
  catch (err) {
    console.log("ERROR:");
    console.log(err.message);

    if (err.status === 400 || err.message === 'no such assetid' || err.message === 'missing assetid') {
      response.status(400).json({
        "message": err.message,
        "data": []
      });
    } else {
      response.status(500).json({
        "message": err.message,
        data: []
      });
    }
  }

};
