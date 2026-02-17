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
image
s
_
with_label
*
*
@description
when
an image is uploaded to S3, the Rekognition
*
AI service is
automatically called to label objects in the image.
*
These labels
are then stored in the database for retrieval / search.
*
Given a
label (partial such as 'boat' or complete 'sailboat'), this
*
function performs a case
-
insensitive search for all images with
*
this label.
If successful the labels are returned as a JSON object
*
of the
form {message: ..., data: ...} where message is "success" and
* data is a list of dictionary
-
like objects of the form
{"assetid": int,
* "label": string, "confidence": int}, ordered by
assetid and then label.
*
If
an error occurs,
status code of 500 is sent where JSON object's
* message is the error message and the list is empty
.
*
*
@param
label
(required URL parameter)
to search for, can be a partial word (e.g.
boat
)
*
@returns
JSON {message: string, data: [object, object, ...]}
*/
exports.get_images_with_label = async (request, response) => {

  async function try_get_images_with_label() {
    let dbConn = null;
    try {
      const label = request.query.label;

      if (label === undefined) {
        const err = new Error("missing label");
        err.status = 400;
        throw err;
      }

      dbConn = await get_dbConn();
      await dbConn.beginTransaction();

      const lookup_sql = "SELECT labelid, assetid, label, confidence FROM assetlabels WHERE LOWER(label) LIKE LOWER(?) ORDER BY assetid, label";
      const [rows] = await dbConn.execute(lookup_sql, [`%${label}%`]);

      if (rows.length === 0) {
        await dbConn.commit();
        return {
          message: "success",
          data: []
        };
      }

      console.log("getting labels...");

      await dbConn.commit();

      return {
        message: "success",
        data: rows.map(row => ({
          assetid: row.assetid,
          label: row.label,
          confidence: row.confidence
        }))
      };

    } catch (err) {
      console.log("ERROR in try_get_images_with_label:");
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
    console.log("**Call to get images_with_label...");

    const result = await pRetry(try_get_images_with_label, { retries: 2 });

    response.json({
      "message": "success",
      "data": result.data
    });
  }
  catch (err) {
    console.log("ERROR:");
    console.log(err.message);

    if (err.status === 400 || err.message === 'missing label') {
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
